#include "options.h"
#include "db.h"

#include <iostream>
#include <string>

#include <gflags/gflags.h>

using namespace mdb;

DEFINE_bool(recovery, false, 
    "Start the database in recovery mode (use files existing in path as a starting point");

DEFINE_string(path, "./db_files/",
    "Where to store the files that mdb will generate.");

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Options opt{ .path = FLAGS_path };
    DB db(opt);

    /*
    for (long i = 0; i < 10000000; i++) {
        auto key{ "hello" + std::to_string(i) };
        db.Put(key, "world");
        //assert(db.Get(key) == "world");
    }
    */
}
