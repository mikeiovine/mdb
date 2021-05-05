#pragma once

#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "benchmark_interface.h"

namespace mdb {
namespace benchmark {

// Generate of random string of a specified size
std::string RandomString(size_t size);

// Generate a sequence of pairs {key, value}, where each key/value is a random
// string of a fixed size.
std::vector<std::pair<std::string, std::string>> CreateRandomKeyValuePairs(
    size_t num_entries, size_t key_size, size_t value_size);

// Open a new metrics file. Print an error message if the file fails to open.
std::optional<std::ofstream> OpenMetricsFile(const BenchmarkOptions& options);

}  // namespace benchmark
}  // namespace mdb
