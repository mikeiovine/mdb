#include "table_builder.h"
#include "constants.h"

#include <iostream>

int main() {
    mdb::TableBuilder t("test.sst");
    t.add("a", "abc");
}
