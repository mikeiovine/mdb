#include "file.h"
#include "log_writer.h"
#include "posix.h"

#include <iostream>

using namespace mdb;

int main() {
    auto e = CreateEnv();
    LogWriter(e->MakeWriteOnlyIO("test.txt"));
}
