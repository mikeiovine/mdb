#include "helpers.h"

#include <cassert>
#include <filesystem>
#include <regex>

#include "options.h"

namespace mdb {
namespace util {

void AddStringToWritable(std::string_view str, std::vector<char>& writable) {
  size_t str_size{str.size()};
  char* size_bytes{reinterpret_cast<char*>(&str_size)};

  writable.insert(writable.end(), size_bytes, size_bytes + sizeof(size_t));

  writable.insert(writable.end(), str.begin(), str.end());
}

std::string LogFileName(const Options& options, size_t number) {
  return options.path / ("log" + std::to_string(number) + ".dat");
}

std::string TableFileName(const Options& options, size_t number) {
  return options.path / ("table" + std::to_string(number) + ".mdb");
}

FileInfo GetFileInfo(const std::filesystem::directory_entry& entry) {
  if (!std::filesystem::is_regular_file(entry)) {
    return {.id = FileType::Unknown, .index = 0, .path = entry.path()};
  }

  std::string fname{entry.path().filename()};

  const std::regex log_regex{"log([0-9]+)\\.dat"};
  const std::regex table_regex{"table([0-9]+)\\.mdb"};
  std::smatch base_match;

  auto id{FileType::Unknown};
  size_t index{0};

  if (std::regex_match(fname, base_match, log_regex)) {
    assert(base_match.size() == 2);
    id = FileType::LogFile;
    index = std::stoi(base_match[1].str());
  } else if (std::regex_match(fname, base_match, table_regex)) {
    assert(base_match.size() == 2);
    id = FileType::TableFile;
    index = std::stoi(base_match[1].str());
  }

  return {.id = id, .index = index, .path = entry.path()};
}

}  // namespace util
}  // namespace mdb
