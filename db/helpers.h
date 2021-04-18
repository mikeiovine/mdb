#pragma once

#include <vector>
#include <string>

namespace mdb {
namespace util {

void AddStringToWritable(const std::string& str, std::vector<char>& writable);

} // namespace util
} // namespace mdb
