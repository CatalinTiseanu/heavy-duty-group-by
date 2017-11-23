## Description
==============

The task was to build a function which can group a set of key-value:

def groupBy(input: Iterator[(K, V)]): Iterator[(K, List[V])]

The requirements were:
* Function should work even if the total size of the input exceeds the size of the memory. (I assumed constant key and value size)
* Performance should degrade gracefully as the problem size varies. (if the input can fit easily into memory, don't use disk)
* Algorithm should not exceed O(n log n) average case performance for n input key-value pairs
* Code shoudl be thread-safe

## Algorithm
============

First, create a temporary folder associated with the request, which will hold all dump files.

1) Stage 1: Go through the input maintaining a hashmap of the form key -> list(values)
If at any point the hashmap exceed max_hashmap_entries dump it to disk (see _dump_hashmap_to_disk)
and clear it
If after Stage 1 there was no dump to disk (input fits in memory) return an KeyValueIteratorFromMemory over
the resulting hashmap.  

2) Stage 2: In case the number of dump files on disk (num_files) exceeds the maximum allowed number
(max_number_of_files) do merges with max_num_files files at a time until at most max_num_files remain.  

3) Stage 3: Return a KeyListIteratorFromDisk over the remaining dump files, which simulates a multi-way merge
(same as in Stage 2). Finally, once the KeyListIteratorFromDisk iterator has been exhausted, remove the associated temporary folder.  

## Time and memory complexity:
==============================

Let kv_size equal the size in memory of a single (key, value) entry.  
Let N equal the total number of (key, value) entries in the input.  
Total memory consumption: `O(max(max_hashmap_entries, max_num_files) * kv_size)` . 
Total disk space consumption: (N * kv_size) . 

Total execution time (including full iteration over the result):  
* `O(N log(N)` if the stream fits in memory (N <= max_hashmap_entries) . 
Else if the stream doesn't fit in memory: 
* `O(N log(N)` if the stream fits in memory (N <= max_hashmap_entries) . 
* `O(N log(max_hashmap_entries))` for chunking the stream into hashmaps stored on disk . 
* `O(N log(N)` if the stream fits in memory (N <= max_hashmap_entries) . 
* `O(N log_in_base_(max_num_files)_of(N / max_hashmap_entries))` for merging the dump files until at most
* `O(N log(N)` if the stream fits in memory (N <= max_hashmap_entries) . 
        max_num_files remain . 
* `O(N log (max_num_files))` for iterating over the result . 
* `O(N log(N)` if the stream fits in memory (N <= max_hashmap_entries) . 
In total:  
    * `O(N * (log(max_hashmap_entries) + log_in_base_(max_num_files)_of(N / max_hashmap_entries) +
                      log (max_num_files)))` . 

## How to test
==============

Tests are found in the test/ folder.  
To run tests install nose with ./install.sh . 
To run unit tests and doc tests run ./run_tests.sh
