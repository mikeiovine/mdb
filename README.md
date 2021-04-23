# mdb
This is a key-value store implementation that I made for fun.
The design is roughly based on Google's [LevelDB](https://github.com/google/leveldb).
In particular, the KV store is backed by a log-structured merge tree on
disk.

## Dependencies

* [gflags](https://github.com/gflags/gflags)
* [googletest](https://github.com/google/googletest) [testing only]

## Basic Usage
```cpp
// Setup options

Options opt{
    .path = "./path/to/db_files/",
    .write_sync = false
};

DB db(opt);

// Put a key
db.Put("some key", "some value");

// Get a key
db.Get("some key");
```
