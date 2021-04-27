#include "helpers.h"

#include <filesystem>

#include "options.h"

namespace mdb {
namespace util {

void AddStringToWritable(std::string_view str, std::vector<char>& writable) {
  size_t str_size{str.size()};
  char* size_bytes{reinterpret_cast<char*>(&str_size)};

  writable.insert(writable.end(), size_bytes, size_bytes + sizeof(size_t));

  writable.insert(writable.end(), str.begin(), str.end());
}

std::string LogFileName(const Options& options, int number) {
  return options.path / ("log" + std::to_string(number) + ".dat");
}

std::string TableFileName(const Options& options, int number) {
  return options.path / ("table" + std::to_string(number) + ".mdb");
}

}  // namespace util
}  // namespace mdb
