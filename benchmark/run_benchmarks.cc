#include <boost/log/core/core.hpp>
#include <boost/program_options.hpp>
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>

#include "benchmark_interface.h"

using namespace mdb::benchmark;

namespace po = boost::program_options;

using BenchmarkMap =
    std::unordered_map<std::string, std::shared_ptr<Benchmark>>;
using BenchmarkList =
    std::vector<std::pair<std::string, std::shared_ptr<Benchmark>>>;

BenchmarkMap GetBenchmarkMap(const BenchmarkOptions &options) {
  // This map must be updated when adding a new benchmark!
  BenchmarkMap map{
      {"write_random", std::make_shared<WriteRandomBenchmark>(options)},
      {"read_random", std::make_shared<ReadRandomBenchmark>(options)}};

  return map;
}

BenchmarkList CreateBenchmark(const std::string &benchmark,
                              const BenchmarkOptions &options) {
  auto map{GetBenchmarkMap(options)};

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

void ValidateBenchmark(const std::string &value) {
  auto map{GetBenchmarkMap(BenchmarkOptions())};

  if (map.find(value) == map.end() && !value.empty()) {
    std::cerr << "Invalid benchmark option. Valid options are:\n";
    for (const auto &benchmark : map) {
      std::cerr << "    " << benchmark.first << '\n';
    }

    throw po::validation_error(po::validation_error::invalid_option_value,
                               "benchmark");
  }
}

void ValidatePositiveInt(const std::string &name, int64_t value) {
  if (value <= 0) {
    std::cerr << name << " must be > 0!\n";
    throw po::validation_error(po::validation_error::invalid_option_value,
                               name);
  }
}

int main(int argc, char *argv[]) {
  po::options_description desc{"Usage"};
  desc.add_options()
      ("help", "Show this help message")
      ("benchmark", po::value<std::string>()->default_value("")->notifier(&ValidateBenchmark), "The benchmark to run. If empty, run all benchmarks")
      ("num_entries", po::value<int64_t>()->default_value(1e6)->notifier([](int64_t value) { ValidatePositiveInt("num_entries", value); }), "Number of entries to write/read")
      ("key_size", po::value<int64_t>()->default_value(16)->notifier([](int64_t value) { ValidatePositiveInt("key_size", value); }), "Key size in bytes")
      ("value_size", po::value<int64_t>()->default_value(100)->notifier([](int64_t value) { ValidatePositiveInt("value_size", value); }), "Value size in bytes")
      ("write_metrics", po::bool_switch(), "If set, write benchmark metrics to ./metrics/")
 ;

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  if (vm.count("help")) {
    std::cout << desc << '\n';
    return 1;
  }

  BenchmarkOptions opt{.write_metrics = vm["write_metrics"].as<bool>(),
                       .key_size = vm["key_size"].as<int64_t>(),
                       .value_size = vm["value_size"].as<int64_t>(),
                       .num_entries = vm["num_entries"].as<int64_t>()};

  auto benchmarks{CreateBenchmark(vm["benchmark"].as<std::string>(), opt)};

  boost::log::core::get()->set_logging_enabled(false);

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
