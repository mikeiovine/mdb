#include "log_file.h"

#include <cassert>

namespace mdb {

LogFile::LogFile(const std::string& filename) : 
    output_{ std::make_unique<WritableFile>(filename) } {}

LogFile::~LogFile() = default;

void LogFile::write_with_sep(const std::string& s) {
    output_->write(s.c_str(), s.size());
    output_->write(&LogFile::SEPARATOR, 1);
}

void LogFile::write_with_sep(const char * s, size_t size) {
    output_->write(s, size);
    output_->write(&LogFile::SEPARATOR, 1);
}

void LogFile::add(const std::string& key, const std::string& value) {
    assert(key.size() != 0);

    auto key_size = key.size();
    write_with_sep(
        reinterpret_cast<const char *>(&key_size),
        sizeof(key_size));

    write_with_sep(key);

    auto value_size = value.size();
    write_with_sep(
        reinterpret_cast<const char *>(&value_size), 
        sizeof(value_size));

    // Empty value corresponds to deleted key
    if (value.size() > 0) {
        write_with_sep(value);
    }
}

} // namespace mdb
