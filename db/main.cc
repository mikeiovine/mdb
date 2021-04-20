#include "options.h"
#include "db.h"

#include <iostream>
#include <string>

using namespace mdb;

int main() {
    Options opt;
    opt.path = "db_files/";
    DB db(opt);

    for (long i = 0; i < 1000000000000; i++) {
        db.Put("hello" + std::to_string(i), "world");
    }
}
