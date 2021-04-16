#include "file.h"
#include "env.h"
#include "options.h"
#include "log_writer.h"

#include <iostream>
#include <string>

using namespace mdb;

int main() {
    auto e = Env::CreateDefault();
    LogWriter l(e->MakeWriteOnlyIO("test.txt"), false);
    l.MarkDelete("x");

    auto options = MDBOptions(false);
}
