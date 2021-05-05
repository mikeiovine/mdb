#include "benchmark_util.h"

#include <iostream>

namespace mdb {
namespace benchmark {

std::string RandomString(size_t size) {
  std::string output;
  output.reserve(size);

  static const char* possible_characters =
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "1234567890!@#$%^&*()";

  for (size_t i = 0; i < size; i++) {
    size_t idx{std::rand() % (sizeof(possible_characters) - 1)};
    output += possible_characters[idx];
  }

  return output;
}

std::vector<std::pair<std::string, std::string>> CreateRandomKeyValuePairs(
    size_t num_entries, size_t key_size, size_t value_size) {
  std::vector<std::pair<std::string, std::string>> key_values;
  for (size_t i = 0; i < num_entries; i++) {
    key_values.emplace_back(RandomString(key_size), RandomString(value_size));
  }
  return key_values;
}

std::optional<std::ofstream> OpenMetricsFile(const BenchmarkOptions& options) {
  auto filename{options.metrics_path / (options.metrics_filename + ".csv")};
  std::ofstream metrics{filename};

  if (options.write_metrics) {
    if (!metrics.is_open()) {
      std::cerr << "WARNING: write_metrics is set to true, but the file "
                << filename
                << " failed to open. Metrics will not be written.\n";

      return std::nullopt;
    }
  }

  return metrics;
}

}  // namespace benchmark
}  // namespace mdb
