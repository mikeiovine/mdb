#pragma once

#include <vector>
#include <string>

namespace mdb {

class Options;

namespace util {

void AddStringToWritable(std::string_view str, std::vector<char>& writable);

std::string LogFileName(const Options& options, int number);

std::string TableFileName(const Options& options, int number);

} // namespace util
} // namespace mdb
