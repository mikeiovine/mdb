#include "log_reader.h"

#include <cassert>
#include <vector>

namespace mdb {

LogReader::LogReader(std::unique_ptr<ReadOnlyIO> file) : 
    file_{ std::move(file) } {
    assert(file_ != nullptr);
}

std::optional<std::string> LogReader::ReadNextString() {
    assert(file_ != nullptr);

    std::vector<char> buf;
    buf.reserve(sizeof(size_t));

    // TODO: Log corruption where nullopt is returned
    if (file_->ReadNoExcept(buf.data(), sizeof(size_t)) != sizeof(size_t)) {
        return std::nullopt;
    }

    size_t str_size{ *reinterpret_cast<size_t*>(buf.data()) };

    if (str_size == 0) {
        return "";
    }
    
    try {
        buf.reserve(str_size);
    } catch (const std::bad_alloc&) {
        return std::nullopt;
    }

    if (file_->ReadNoExcept(buf.data(), str_size) != str_size) {
        return std::nullopt;
    }

    return std::string{ buf.data(), str_size };
}

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
