#pragma once

#include <string>
#include <vector>

namespace mdb {
namespace benchmark {

// Generate of random string of a specified size
std::string RandomString(size_t size);

// Generate a sequence of pairs {key, value}, where each key/value is a random
// string of a fixed size.
std::vector<std::pair<std::string, std::string>> CreateRandomKeyValuePairs(
    size_t num_entries, size_t key_size, size_t value_size);

}  // namespace benchmark
}  // namespace mdb
