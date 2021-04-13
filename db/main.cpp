#include "log_file.h"
#include "constants.h"

#include <iostream>

int main() {
    mdb::LogFile t("test.txt");
    
    t.add("hello", "");
}
