#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace mdb {

struct Options;

namespace util {

// Given a string "str", append the following sequence of bytes to
// the buffer in "writable": str.size() + str.data() [no null terminator!]
void AddStringToWritable(std::string_view str, std::vector<char>& writable);

// Produce the n-th logfile name, "/path/in/options/logn.dat"
std::string LogFileName(const Options& options, size_t number);

// Produce the n-th table file name, "/path/in/options/tablen.mdb"
std::string TableFileName(const Options& options, size_t number);

enum class FileType { LogFile, TableFile, Unknown };

struct FileInfo {
  FileType id;
  size_t index;
  std::filesystem::path path;
};

FileInfo GetFileInfo(const std::filesystem::directory_entry& entry);

}  // namespace util
}  // namespace mdb
