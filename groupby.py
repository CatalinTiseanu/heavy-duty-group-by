
import os
import shutil
import random
import logging
import gc
import sys

from collections import defaultdict
from datetime import datetime

from test.test_utils import ListIterator
from iterators import KeyListIteratorFromMemory, KeyListIteratorFromDisk, MergeFileIterator


class GroupByStatement(object):
    """
    Acts as configuration wrapper for the groupBy method.
    Enables sharing of state across all the helper methods used by groupBy.

    >>> g = GroupByStatement(max_num_files=10, max_hashmap_entries=1000)
    >>> it = g.groupBy(ListIterator([(1, 0), (0, 1), (1, 2), (5, 7)]))
    >>> it.next()
    (0, ['1'])
    >>> it.next()
    (1, ['0', '2'])
    >>> it.hasNext()
    True
    >>> g.remove_log()
    """

    def __init__(self, max_num_files=100, max_hashmap_entries=1000000, max_memory=-1, request_id=None):
        """
        :param max_num_files: The maximum number of files to merge at one step using MergeFileIterator
        :param max_hashmap_entries: The maximum number of (key, value) entries to store in memory before
                                    dumping to disk
        :param max_memory: The maximum amount of memory (in bytes) allocated to groupBy.
                           If different from -1, will automatically change the values for max_hashmap_entries and
                           max_num_files once the first (key, value) has been processed (in order to be able to compute
                           the size of (key, value) pair.
        :param request_id: Used for testing
        """

        self._num_files = 0
        self._max_num_files = max_num_files
        self._max_hashmap_entries = max_hashmap_entries
        self._request_id = request_id
        self._max_memory = max_memory
        self._logger = None
        # number of hashmap writes on disk during _chunk_input_into_dump_files
        self.spills = 0
        # number of merge stages done to reduce the number of dump files to less than equal to max_num_files
        self.num_merge_stages = 0
        # number of processed entries - (key, value) pairs
        self.total_num_entries = 0

    def _get_dump_filename(self, index):
        """
        Helper method which returns the path of a dump file given it's index
        """
        return "{}/dump_{}".format(self._request_id, index)

    def _get_merge_filename(self):
        """
        Helper method which returns the path of the merge file used by _merge_dump_files
        """
        return "{}/_merge".format(self._request_id)

    @staticmethod
    def write_key_values_to_file(key_values_list, filename):
        f = open(filename, "w")
        for key, values in key_values_list:
            f.write("{}\n".format(key))
            f.write(" ".join([str(v) for v in values]) + "\n")
        f.close()

    def _merge_dump_files(self):
        """
        Merge the number of dump files until at most _max_num_files remain by merging at most max_num_files at once

        For example, if we start with _num_files = 100 and _max_num_files = 5 we will need 2 passes:
        1. After the first pass we will have 20 dump files remaining
        2. After the second pass we will have 4 dump files remaining
        """
        self._logger.info("Number of dump files after _chunk_input_into_dump_files: {}".format(self._num_files))

        while self._num_files > self._max_num_files:
            # Since we need to merge dump files we need to be able to merge at least 2 at one step, therefore
            # self._max_num_files has to be greater than 1
            if self._max_num_files < 2:
                error_msg = "Unable to merge dump files: max_num_files has to be greater than 1"
                self._logger.error(error_msg)
                # Clean up
                shutil.rmtree(self._request_id)
                raise error_msg

            current_merge_file = 0

            # Merge at most _max_num_files at once
            for index in range(0, self._num_files, self._max_num_files):
                # List of current files to be merged into one
                filename_list = [self._get_dump_filename(file_id)
                                 for file_id in range(index, min(self._num_files, index + self._max_num_files))]
                merge_filename = self._get_merge_filename()
                dump_filename = self._get_dump_filename(current_merge_file)

                if len(filename_list) == 1:
                    shutil.move(filename_list[0], dump_filename)
                else:
                    self.write_key_values_to_file(MergeFileIterator(filename_list), merge_filename)

                    # Remove dump files and rename the merge file to a dump file
                    for filename in filename_list:
                        os.remove(filename)

                    # Recycle merged dump files
                    shutil.move(merge_filename, dump_filename)

                current_merge_file += 1

            self._logger.info(
                "At merge stage {} merged {} dump files into {}".format(self.num_merge_stages, self._num_files,
                                                                        current_merge_file))

            self._num_files = current_merge_file
            self.num_merge_stages += 1

        self._logger.info("Number of dump files after _merge_dump_files: {}".format(self._num_files))

    def _dump_hashmap_to_disk(self, hashmap, filename):
        """
        Dumps the hashmap to disk and then clears it.
        Each line of the dump will correspond to one (key, value) pair, in the format 'key value'.
        Within the file the keys are going to be ordered in ascending order.

        Stream [(1, 0), (2, 3), (1, 1), (2, 1), (3, 5)] is going to represented as
        hashmap {1: [0, 1], 2: [3, 1], 3 : [5]} which is going to be stored as a dump file consisting of 5 lines:

        '
        1 0
        1 1
        2 3
        2 1
        3 5
        '

        :param hashmap: the hashmap to dump to disk and then clear it
        :param filename: the filename of the dump
        """

        self.write_key_values_to_file([(key, hashmap[key]) for key in sorted(hashmap.keys())], filename)
        self._num_files += 1
        hashmap.clear()
        gc.collect()

        self.spills += 1

    def _chunk_input_into_dump_files(self, input_iterator):
        """
        Chunks the input stream into hashmaps of key: list(values).
        Every time the number of entries in the hashmaps is greater than equal to max_num_entries it spills the hashmap
        on disk and clears it.

        For the case in which no spills are necessary the (only) hashmap is returned.

        :param input_iterator: input stream iterator
        """

        # current_num_entries counts the number of entries of type (key, value) present in the hashmap stored in
        # memory
        current_num_entries = 0
        current_hashmap = defaultdict(list)

        while input_iterator.hasNext():
            # Check if the current hashmap is too large to fit in memory
            if current_num_entries >= self._max_hashmap_entries:
                # Dump hashmap on disk and then clear it
                self._dump_hashmap_to_disk(current_hashmap, self._get_dump_filename(self._num_files))
                current_num_entries = 0

            key, value = next(input_iterator)
            self.total_num_entries += 1

            if self._max_memory > 0 and self.total_num_entries == 1:
                kv_size = sys.getsizeof(key) + sys.getsizeof(value)
                self._max_hashmap_entries = self._max_memory // kv_size
                self._max_num_files = self._max_memory // kv_size

                # Add a maximum limit of 1000 for max_num_files
                self._max_num_files = min(1000, self._max_num_files)

                if self._logger:
                    self._logger.info("Setting max_hashmap_entries={} and max_num_files={} based on max_memory={}bytes"
                                      .format(self._max_hashmap_entries, self._max_num_files, self._max_memory))

            current_hashmap[key].append(str(value))
            current_num_entries += 1

        if self._num_files == 0:
            # The whole input fits in memory
            return current_hashmap
        else:
            if current_num_entries:
                # Dump the last hashmap to disk
                self._dump_hashmap_to_disk(current_hashmap, self._get_dump_filename(self._num_files))

    def groupBy(self, input_iterator):
        """
        Computes a groupBy of the given stream by key.
        Assumes keys of type int and values of type string.

        Uses the following algorithm:

        First, create a temporary folder associated with the request, which will hold all dump files.

        1) Stage 1: Go through the input maintaining a hashmap of the form key -> list(values)
           If at any point the hashmap exceed max_hashmap_entries dump it to disk (see _dump_hashmap_to_disk)
           and clear it

        If after Stage 1 there was no dump to disk (input fits in memory) return an KeyValueIteratorFromMemory over
        the resulting hashmap.

        2) Stage 2: In case the number of dump files on disk (num_files) exceeds the maximum allowed number
        (max_number_of_files) do merges with max_num_files files at a time until at most max_num_files remain.

        3) Stage 3: Return a KeyListIteratorFromDisk over the remaining dump files, which simulates a multi-way merge
        (same as in Stage 2).

        Finally, once the KeyListIteratorFromDisk iterator has been exhausted, remove the associated temporary folder.

        Let kv_size equal the size in memory of a single (key, value) entry.
        Let N equal the total number of (key, value) entries in the input.

        Total memory consumption: O(max(max_hashmap_entries, max_num_files) * kv_size)
        Total disk space consumption (N * kv_size)

        Total execution time (including full iteration over the result):
            * O(N log(N) if the stream fits in memory (N <= max_hashmap_entries)
            else
               * O(N log(max_hashmap_entries)) for chunking the stream into hashmaps stored on disk
               * O(N log_in_base_(max_num_files)_of(N / max_hashmap_entries)) for merging the dump files until at most
                 max_num_files remain
               * O(N log (max_num_files)) for iterating over the result
            In total:
               O(N * (log(max_hashmap_entries) + log_in_base_(max_num_files)_of(N / max_hashmap_entries) +
                      log (max_num_files)))


        :param input_iterator: iterator for the input stream
        :return:
        1) KeyListIteratorFromMemory if the input data fits in memory
        2) KeyListIteratorFromDisk if the input data spills on disk
        """

        self._num_files = 0
        self.total_num_entries = 0
        self.spills = 0
        self.num_merge_stages = 0

        # If input is empty return before creating a temporary folder
        if not input_iterator.hasNext():
            return KeyListIteratorFromMemory({})

        if (self._request_id is None) or os.path.isdir(self._request_id):
            # Compute an unique request id by using the current time (millisecond precision) and a random number
            while True:
                self._request_id = datetime.utcnow().strftime(
                    'request_%Y%m%d_%H%M%S_%f' + str(random.randint(0, (1 << 30))))[:-3]
                if not os.path.isdir(self._request_id):
                    break

        # All files related to this request are going to be stored under this folder
        os.mkdir(self._request_id)

        # Initialize a logger for this request
        logging.basicConfig(filename="{}.log".format(self._request_id),
                            format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
        self._logger = logging.getLogger(self._request_id)
        self._logger.info("Request id: {}".format(self._request_id))

        # Consume the whole stream and chunk it into hashmaps
        result = self._chunk_input_into_dump_files(input_iterator)

        self._logger.info("Processed {} (key, value) pairs".format(self.total_num_entries))
        # If the whole stream fits in memory we are done
        if result is not None:
            self._logger.info("The whole input fits in memory")
            self._logger.info("Returned a KeyListIteratorFromMemory")
            shutil.rmtree(self._request_id)
            return KeyListIteratorFromMemory(result)

        self._logger.info("Did {} dumps of the hashmap on disk".format(self._num_files))
        # Merge the dump files by key until at most _max_num_files remain
        self._merge_dump_files()

        self._logger.info("Returned a KeyListIteratorFromDisk")
        # At this point there are at most _max_num_files dump files in the current request folder
        return KeyListIteratorFromDisk(self._request_id,
                                       [self._get_dump_filename(index) for index in range(self._num_files)])

    def remove_log(self):
        """
        Removes the log for the current request
        """
        try:
            os.remove("{}.log".format(self._request_id))
        except:
            pass


def groupBy(input_iterator,
            max_num_files=1000,
            max_hashmap_entries=10000000,
            max_memory=-1,
            request_id=None,
            keep_log=False):
    """
    Wrapper function for the GroupByStatement class.
    Used in order to guarantee thread-safety in case different users use the same GroupByStatement
    See the relevant documentation for GroupByStatement.__init__ and GroupByStatement.groupBy

    :param keep_log: If set to False remove the log after a successful execution

    >>> it = groupBy (ListIterator([(1, 0), (0, 1), (1, 2), (5, 7)]),\
                      max_num_files=10,\
                      max_hashmap_entries=1000,\
                      max_memory = (1<<30),\
                      keep_log=False)
    >>> it.next()
    (0, ['1'])
    >>> it.next()
    (1, ['0', '2'])
    >>> it.hasNext()
    True
    >>> it.next()
    (5, ['7'])
    >>> it.hasNext()
    False
    """

    g = GroupByStatement(max_num_files, max_hashmap_entries, max_memory, request_id)
    result_iterator = g.groupBy(input_iterator)
    if not keep_log:
        g.remove_log()
    return result_iterator

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
