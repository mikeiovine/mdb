#include "options.h"
#include "db.h"

#include <iostream>
#include <string>

using namespace mdb;

int main() {
    Options opt;
    opt.path = "db_files/";
    DB db(opt);

    for (long i = 0; i < 10000000; i++) {
        auto key{ "hello" + std::to_string(i) };
        db.Put(key, "world");
        assert(db.Get(key) == "world");
    }
}
