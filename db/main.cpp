#include "file.h"
#include "log_writer.h"
#include "posix.h"

#include <iostream>

using namespace mdb;

int main() {
    PosixEnv e;
    LogWriter(e.MakeWriteOnlyIO("test.txt"));
}
