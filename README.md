# LSM Tree - Rust

This is a toy LSM Tree I made to practice rust.

This LSM Tree is a key->value store where both, keys and values are strings.
Supported operations are:

```
1. get
2. upsert
```

## Algorithm

The implemented algorithm is an oversimplified version of how lsm trees are implemented in production, like in scylla db.

In this implementation, The LSM Tree maintains an in memory map of key->value pairs called memtable, and after this memtable reaches a certain size threshold (that is hardcoded to be 100 pairs), the data is flushed into a SSTable (Sorted String Table) on disk.

A summary file is also maintained to keep track of which key is where, and how many data files
previously existed, so that data saved by a previous instance can be used by a newer instance.

Data consolidation, deletion are not implemented.

SSTable format is not interoperable, it's a custom format where every entry is like:

```
<key_len><value_len><key><value>
```

(Note that due to this format, no separator is needed).

Summary file is also same except that it starts with one i64, representing number of data files
associated with this summary that exists.

Data file name is of the format: `{summary_file_name}_data_{file_num}`

This is how summary file is connected to data files.

Note that summary file is vastly inefficient and there's a lot of scope to improve it by utilizing the properties of sstable, and even bloom filters.
