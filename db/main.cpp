#include "file.h"
#include "constants.h"

#include <iostream>

int main() {
    mdb::WritableFile t("test.txt");

    for (int i = 0; i < 1000000; i++) {
        t.write("abc", 3);
    }
}
