#include "log_file.h"

#include <cassert>

namespace mdb {

LogFile::LogFile(const std::string& filename) : output_{ filename } {}

LogFile::~LogFile() = default;

void LogFile::write_with_sep(const std::string& s) {
    output_.write(s.c_str(), s.size());
    output_.write(&LogFile::SEPARATOR, 1);
}

void LogFile::add(const std::string& key, const std::string& value) {
    assert(key.size() != 0);

    write_with_sep(std::to_string(key.size()));
    write_with_sep(key);

    write_with_sep(std::to_string(value.size()));

    // Empty value corresponds to deleted key
    if (value.size() > 0) {
        write_with_sep(value);
    }
}

} // namespace mdb
