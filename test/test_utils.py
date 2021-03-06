
from iterators import JavaIterator


class IncrementalKeyValueIterator(JavaIterator):
    """
    Generates a sequence of keys in the range (0, key_range-1) and values in range (0, value_range-1).
    At each step it increments the current_key by key_jump and takes the result modulo key_range.
    Same for current_value.

    >>> it = IncrementalKeyValueIterator (3, 2, 3)
    >>> it.next()
    (0, 0)
    >>> it.next()
    (1, 1)
    >>> it.hasNext()
    True
    >>> it.next()
    (0, 2)
    >>> it.hasNext()
    False
    >>> it.next()
    Traceback (most recent call last):
    ...
    StopIteration
    """

    def __init__(self, nr_pairs, key_range, value_range, key_jump=1, value_jump=1):
        # remaining number of (key, value) pairs to iterate over
        self.nr_pairs = nr_pairs

        # current_key will always be in 0 ... key_range-1
        self.key_range = key_range
        # current_value will always be in 0 ... key_value-1
        self.value_range = value_range

        # key to be returned by next()
        self.current_key = 0
        # value to be returned by next()
        self.current_value = 0

        # increment of current_key after next()
        self.key_jump = key_jump
        # increment of current_value after next()
        self.value_jump = value_jump

    def hasNext(self):
        return self.nr_pairs > 0

    def __next__(self):
        if self.hasNext():
            key = self.current_key
            value = self.current_value
            self.current_key = (self.current_key + self.key_jump) % self.key_range
            self.current_value = (self.current_value + self.value_jump) % self.value_range

            self.nr_pairs -= 1
            return key, value
        else:
            raise StopIteration()


class ListIterator(JavaIterator):
    """
    Adds the hasNext() method to the standard list iterator

    >>> it = ListIterator([(1, 0), (2, 1), (3, 0)])
    >>> it.next()
    (1, 0)
    >>> it.next()
    (2, 1)
    >>> it.next()
    (3, 0)
    >>> it.hasNext()
    False
    """
    def __init__(self, l):
        self.nr_pairs = len(l)
        self._iter = iter(l)

    def hasNext(self):
        return self.nr_pairs > 0

    def __next__(self):
        self.nr_pairs -= 1
        return next(self._iter)

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)