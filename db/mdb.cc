#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "db.h"
#include "options.h"

using namespace mdb;

DEFINE_bool(recovery, false,
            "Start the database in recovery mode (use files existing in path "
            "as a starting point");

DEFINE_string(path, "./db_files/",
              "Where to store the files that mdb will generate.");

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  Options opt{.path = FLAGS_path};
  DB db(opt);

  for (long i = 0; i < 1500000; i++) {
    auto key{"hello" + std::to_string(i)};
    db.Put(key, "world");
    if (i >= 1000) {
      db.Delete("hello" + std::to_string(i - 1000));
    }
  }

  db.WaitForOngoingCompactions();
  std::cout << "finished, running assertions" << std::endl;

  for (long i = 0; i < 1000000; i++) {
    assert(db.Get("hello" + std::to_string(i)) == "");
  }
}
