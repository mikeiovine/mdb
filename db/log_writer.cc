#include <cassert>

#include "log_writer.h"

namespace mdb {

LogWriter::LogWriter(std::unique_ptr<WriteOnlyIO> file) : file_{ std::move(file) } {
    assert(file_ != nullptr);
}

void LogWriter::Add(const std::string& key, const std::string& value) {
    Append(key);
    Append(value);
}

void LogWriter::Append(const std::string& data) {
}

} // namespace mdb
