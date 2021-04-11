#include "table_builder.h"
#include "constants.h"

#include <iostream>

namespace mdb {

TableBuilder::TableBuilder(const std::string& filename) : output_{ filename } {}

void TableBuilder::add(const std::string& key, const std::string& value) {
    output_ << key << mdb::constants::KV_SEP << value << mdb::constants::PAIR_SEP;
}

} // namespace mdb
