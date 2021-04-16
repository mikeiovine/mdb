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

    if (!file_->Read(buf.data(), sizeof(size_t))) {
        return std::nullopt;
    }

    size_t str_size{ *reinterpret_cast<size_t*>(buf.data()) };

    if (str_size == 0) {
        return "";
    }
    
    buf.reserve(str_size);
    file_->Read(buf.data(), str_size);

    return std::string{ buf.data(), str_size };
}

}
