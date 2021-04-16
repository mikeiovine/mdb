#pragma once

#include <cassert>

namespace mdb {

template <class MemTableT>
MemTableT LogReader::ReadMemTable() {
    MemTableT memtable;

    auto key = ReadNextString();
    auto value = ReadNextString();

    while (key && value) {
        if (value->size() > 0) {
            memtable.insert_or_assign(key.value(), value.value());
        } else {
            memtable.erase(key.value());
        }

        key = ReadNextString();
        if (key) {
            value = ReadNextString();
        }
    }

    return memtable;
}

} // namespace mdb
