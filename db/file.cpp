#include "file.h"
#include "constants.h"

#include <iostream>
#include <unistd.h>
#include <fcntl.h>

namespace mdb {

WritableFile::WritableFile(const std::string& filename) {
    fd_ = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
    if (fd_ == -1) {
        throw std::system_error(errno, std::generic_category());
    }
}

void WritableFile::add(const std::string& key, const std::string& value) {
    write(key.c_str(), key.size());
    write(&mdb::constants::KV_SEP, 1);
    write(value.c_str(), value.size());
    write(&mdb::constants::PAIR_SEP, 1);
}

void WritableFile::write(const char * data, size_t size) {
    if (::write(fd_, data, size) == -1) {
        throw std::system_error(errno, std::generic_category());
    }
}

} // namespace mdb
