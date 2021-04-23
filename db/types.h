#pragma once

#include <map>
#include <string>

// Helpful type aliases
namespace mdb {

// Heterogeneous lookup is enabled via std::less<>. This
// allows us to do memtable.find(std::string_view) without
// constructing a std::string.
using MemTableT = std::map<std::string, std::string, std::less<>>;

using IndexT = std::map<std::string, size_t, std::less<>>;

};  // namespace mdb
