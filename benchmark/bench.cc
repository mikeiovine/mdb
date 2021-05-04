#include <gflags/gflags.h>

#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>

#include "benchmark_interface.h"

using BenchmarkMap =
    std::unordered_map<std::string, std::shared_ptr<Benchmark>>;
using BenchmarkList =
    std::vector<std::pair<std::string, std::shared_ptr<Benchmark>>>;

BenchmarkMap GetBenchmarkMap() {
  // This map must be updated when adding a new benchmark!
  BenchmarkMap map{
      {"write_random", std::make_shared<WriteRandomBenchmark>(
                           WriteRandomBenchmark::GetBenchmarkOptions())}};

  return map;
}

BenchmarkList CreateBenchmark(const std::string &benchmark) {
  auto map{GetBenchmarkMap()};

  BenchmarkList list;

  // Empty -> return all benchmarks
  if (benchmark.empty()) {
    std::for_each(map.cbegin(), map.cend(),
                  [&list](const auto &pair) { list.push_back(pair); });

  } else {
    auto it{map.find(benchmark)};
    assert(it != map.end());

    list.push_back(*it);
  }

  return list;
}

bool ValidateBenchmark(const char * /*flagname*/, const std::string &value) {
  auto map{GetBenchmarkMap()};

  if (map.find(value) == map.end() && !value.empty()) {
    std::cerr << "Invalid benchmark option. Valid options are:\n";
    for (const auto &benchmark : map) {
      std::cerr << "    " << benchmark.first << '\n';
    }
    return false;
  }

  return true;
}

// Command line flags
DEFINE_string(benchmark, "",
              "The benchmark to run. If empty, run all benchmarks");
DEFINE_validator(benchmark, &ValidateBenchmark);

DEFINE_uint32(key_size, 16, "Key size in bytes");
DEFINE_uint32(value_size, 100, "Value size in bytes");
DEFINE_uint32(num_entries, 1e6, "Number of entries to write/read");

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto benchmarks{CreateBenchmark(FLAGS_benchmark)};

  for (const auto &benchmark_pair : benchmarks) {
    std::cout << "Running benchmark " << benchmark_pair.first << '\n';

    auto &benchmark{benchmark_pair.second};
    assert(benchmark != nullptr);

    bool success{benchmark->Run()};
    auto stats{benchmark->GetStats()};

    if (!success) {
      std::cerr << "Benchmark failure. Reason: " << stats.failure_reason
                << '\n';
    } else {
      std::cout << "Benchmark executed successfully." << '\n';
      std::cout << "Runtime (seconds): " << stats.time.count() << '\n';
    }
  }
  return 0;
}
