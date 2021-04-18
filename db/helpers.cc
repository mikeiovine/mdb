#include "helpers.h"

namespace mdb {
namespace util {

void AddStringToWritable(const std::string& str, std::vector<char>& writable) {
    size_t str_size{ str.size() };
    char * size_bytes{ reinterpret_cast<char*>(&str_size) };

    writable.insert(
        writable.end(),
        size_bytes,
        size_bytes + sizeof(size_t));

    writable.insert(
        writable.end(),
        str.begin(),
        str.end());
}

} // namespace util
} // namespace mdb
