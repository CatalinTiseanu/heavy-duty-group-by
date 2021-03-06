
import heapq
import shutil
import gc


class JavaIterator(object):
    """
    Base class for iterators over tuples of the form (key, list(values))
    Adds a hasNext() method over the standard Python convention
    """
    def __init__(self):
        raise NotImplementedError

    def __iter__(self):
        return self

    def hasNext(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

    def next(self):
        return self.__next__()

class MergeFileIterator(JavaIterator):
    """
    Iterator over a K-way merge, by key, on K files.
    Each file needs to contain one (key, value) pair per line, ordered non-decreasingly by key.
    Time complexity: O(N log K) where N is the total number of (key, value) pairs across all files.
    Memory complexity: O (K), with a constant proportional to the size of a (key, value) pair
    """

    def __init__(self, filelist):
        self._files = [open(f) for f in filelist]
        self._heap = []

        # Push the first key in each file on the heap
        for index, f in enumerate(self._files):
            key = int(next(f))
            heapq.heappush(self._heap, (key, index, f))

    def hasNext(self):
        return len(self._heap)

    def __next__(self):
        if not self.hasNext():
            raise StopIteration()

        current_key = self._heap[0][0]
        values_list = []

        # Pop all the values for the given current_key
        while self._heap and self._heap[0][0] == current_key:
            key, index, f = heapq.heappop(self._heap)
            # Fetch the values for the given key, which are stored on the next line in f.
            values = next(f).split()
            values_list.extend(values)

            # If f.hasNext(), get the next key push it on the heap.
            try:
                key = int(next(f))
                heapq.heappush(self._heap, (key, index, f))
            except StopIteration:
                # Reached end of file
                f.close()

        return current_key, values_list


class KeyListIteratorFromMemory(JavaIterator):
    """
    KeyListIterator over the result of GroupByWrapper.groupBy for when the data fits into memory.
    Always initialized with a hashmap

    >>> it = KeyListIteratorFromMemory ({1 : [1, 2, 3], 0 : [4, 5, 6], 2 : [1, 2, 3]})
    >>> it.next()
    (0, [4, 5, 6])
    >>> it.next()
    (1, [1, 2, 3])
    >>> it.hasNext()
    True
    >>> it.next()
    (2, [1, 2, 3])
    >>> it.hasNext()
    False
    """
    def __init__(self, hashmap):
        self._hashmap = hashmap
        sorted_keys = sorted(hashmap.keys())
        self._iter = iter(sorted_keys)
        self._remaining_elements = len(sorted_keys)

    def hasNext(self):
        return self._remaining_elements > 0

    def __next__(self):
        key = next(self._iter)
        self._remaining_elements -= 1
        return key, self._hashmap[key]

class KeyListIteratorFromDisk(JavaIterator):
    """
    KeyListIterator for when the input stream spills on disk.
    Wraps the MergeFileIterator.
    Cleans up after it has processed the last element (hasNext() returns false).
    """

    def __init__(self, request_id, file_list):
        """
        :param request_id: Unique request id used to know the relative path of the dump files.
        :param file_list: A list of filenames for the dump files. Should contain at most self._max_num_files
        """
        self._request_id = request_id
        self._merge_file_iterator = MergeFileIterator(file_list)

    def hasNext(self):
        return self._merge_file_iterator.hasNext()

    def __next__(self):
        result = next(self._merge_file_iterator)
        # If we reached the end delete the whole request folder
        if not self.hasNext():
            gc.collect()
            shutil.rmtree(self._request_id)
        return result

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
