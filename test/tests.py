"""
Contains tests for:
* Behaviour of GroupByWrapper.groupBy
* Behaviour of GroupByWrapper.group_input_into_buckets
* Behaviour of MergeFileIterator
"""

import unittest
import copy
import os
import shutil

from iterators import MergeFileIterator
from test.test_utils import IncrementalKeyValueIterator
from groupby import GroupByStatement
from collections import defaultdict


def compute_hashmap(input_iterator):
    """
    Helper functions which first computes a hashmap of key -> list(values) and then returns the list representation,
    sorted by key.

    :param input_iterator: Iterator over a (key, value) container

    >>> compute_hashmap([(0, 0), (1, 1), (0, 2)])
    [(0, [0, 2]), (1, [1])]
    """
    hashmap = defaultdict(list)
    for key, value in input_iterator:
        hashmap[key].append(str(value))
    return [(key, hashmap[key]) for key in sorted(hashmap.keys())]


class GroupByTests(unittest.TestCase):
    def tearDown(self):
        # Clean up in case something went wrong
        current_directories = next(os.walk('.'))[1]
        for dir in current_directories:
            if dir.startswith("test_") or dir.startswith("request_"):
                shutil.rmtree(dir)

    def compare_outputs(self, data_copy, result_iterator):
        expected_output = compute_hashmap(data_copy)
        expected_output_iterator = iter(expected_output)

        entries_seen = 0
        while result_iterator.hasNext():
            self.assertEqual(next(result_iterator), next(expected_output_iterator))
            entries_seen += 1
        self.assertEqual(entries_seen, len(expected_output))

    def test_empty_stream(self):
        g = GroupByStatement(max_num_files=10,
                             max_hashmap_entries=1000,
                             request_id="test_empty_stream")

        data = IncrementalKeyValueIterator(0, 1, 0)
        result = g.groupBy(data)

        self.assertEqual(g.spills, 0)
        self.assertEqual(result.hasNext(), False)

    def test_stream_fits_in_memory(self):
        g = GroupByStatement(max_num_files=10,
                             max_hashmap_entries=1000,
                             request_id="test_stream_fits_in_memory")

        data = IncrementalKeyValueIterator(1000, 10, 7)
        data_copy = copy.deepcopy(data)

        result_iterator = g.groupBy(data)

        self.assertEqual(g.spills, 0)
        self.compare_outputs(data_copy, result_iterator)

    def test_stream_spills_on_disk(self):
        g = GroupByStatement(max_num_files=4,
                             max_hashmap_entries=300,
                             request_id="test_stream_spills_on_disk")

        data = IncrementalKeyValueIterator(1000, 10, 7)
        data_copy = copy.deepcopy(data)

        result_iterator = g.groupBy(data)

        self.assertEqual(g.spills, 4)
        self.compare_outputs(data_copy, result_iterator)

    def test_low_memory(self):
        g = GroupByStatement(max_memory=1024,
                             request_id="test_low_memory")

        data = IncrementalKeyValueIterator(1000, 10, 7)
        data_copy = copy.deepcopy(data)

        result_iterator = g.groupBy(data)

        self.assertTrue(g.spills > 0)
        self.assertTrue(g.num_merge_stages > 0)
        self.assertTrue(g._num_files <= 1000)

        self.compare_outputs(data_copy, result_iterator)

    def test_stream_spills_on_disk_and_file_merges_required(self):
        g = GroupByStatement(max_num_files=2,
                             max_hashmap_entries=100,
                             request_id="test_stream_spills_on_disk_and_file_merges_required")

        data = IncrementalKeyValueIterator(1000, 10, 7)
        data_copy = copy.deepcopy(data)

        result_iterator = g.groupBy(data)

        self.assertEqual(g.spills, 10)
        self.assertEqual(g.num_merge_stages, 3)
        self.assertEqual(g._num_files, 2)

        self.compare_outputs(data_copy, result_iterator)

    def test_large_stream(self):
        g = GroupByStatement(max_num_files=100,
                             max_hashmap_entries=10000,
                             request_id="test_large_stream")

        data = IncrementalKeyValueIterator(200000, 10, 7, 3, 2)
        data_copy = copy.deepcopy(data)

        result_iterator = g.groupBy(data)

        self.assertEqual(g.spills, 20)
        self.assertEqual(g._num_files, 20)

        self.compare_outputs(data_copy, result_iterator)

    def test_chunk_input_into_dump_files(self):
        g = GroupByStatement(max_num_files=100,
                             max_hashmap_entries=100000,
                             request_id="test_chunk_input_into_dump_files")

        data = IncrementalKeyValueIterator(100000, 10, 7)
        data_copy = copy.deepcopy(data)

        result_hashmap = g._chunk_input_into_dump_files(data)
        self.assertEqual(sorted(result_hashmap.items()), compute_hashmap(data_copy))

    def test_single_file_merge(self):
        data = IncrementalKeyValueIterator(9, 9, 2, 3, 1)
        data_copy = copy.deepcopy(data)

        tmp_filename = "data/single_merge"

        GroupByStatement.write_key_values_to_file(compute_hashmap(data), tmp_filename)

        m = MergeFileIterator([tmp_filename])
        self.compare_outputs(data_copy, m)

    def test_multi_file_merge(self):
        num_files = 30
        entries_per_file = [2 * index + 1 for index in range(num_files)]
        N = sum(entries_per_file)

        data = IncrementalKeyValueIterator(N, 23, 11, 11, 2)
        data_copy = copy.deepcopy(data)

        filenames = []

        for index in range(num_files):
            tmp_filename = "data/multi_merge_{}".format(index)
            filenames.append(tmp_filename)

            file_content = defaultdict(list)
            for num_entries in range(entries_per_file[index]):
                key, value = next(data)
                file_content[key].append(value)
            GroupByStatement.write_key_values_to_file([(key, file_content[key]) for key in sorted(file_content.keys())],
                                                      tmp_filename)

        m = MergeFileIterator(filenames)
        self.compare_outputs(data_copy, m)

    def test_consecutive_calls(self):
        g = GroupByStatement(max_num_files=2,
                             max_hashmap_entries=1)

        result_iterator_list = []
        request_id_list = []

        for request_id in range(10):
            data = IncrementalKeyValueIterator(10, 3, 3)
            result_iterator_list.append(g.groupBy(data))
            request_id_list.append(g._request_id)

        for index in range(10):
            # Exhaust iterator
            for key, value in result_iterator_list[index]:
                pass
            self.assertFalse(os.path.isdir(request_id_list[index]))
