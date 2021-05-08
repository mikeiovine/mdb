# mdb
This is a key-value store implementation that I made for fun.
The design is roughly based on Google's [LevelDB](https://github.com/google/leveldb).
In particular, the KV store is backed by a log-structured merge tree on
disk.

This is still a work-in-progress.

## Dependencies

There is only one dependency, [boost](https://www.boost.org/) (>= 1.76.0). To compile
this project, you'll need to install `boost` in a location that 
[FindBoost](https://cmake.org/cmake/help/latest/module/FindBoost.html)
can locate. You must build `Boost.ProgramOptions` since this component
of `boost` is not header-only.

This project will only compile on POSIX compliant operating systems.
I did some benchmarking and found that it was much faster to do file
IO with non-portable syscalls instead of with `iostream`.

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

// Delete a key (it is not an error if the key doesn't exist)
db.Delete("some key");
```
