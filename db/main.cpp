#include "file.h"
#include "env.h"
#include "log_writer.h"

#include <iostream>

using namespace mdb;

int main() {
    auto e = Env::CreateDefault();
    LogWriter(e->MakeWriteOnlyIO("test.txt"));
}
