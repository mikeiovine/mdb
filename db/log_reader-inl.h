#pragma once

#include <cassert>

namespace mdb {

template <class MemTableT>
MemTableT LogReader::ReadMemTable() {
    MemTableT memtable;

    while (auto key = ReadNextString()) {
        auto value = ReadNextString();
        if (value->size() > 0) {
            memtable.insert_or_assign(key.value(), value.value());
        } else {
            memtable.erase(key.value());
        }
    }

    return memtable;
}

} // namespace mdb
