#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>

namespace mdb {
namespace benchmark {

struct BenchmarkStats {
  std::string failure_reason{""};

  std::chrono::seconds time{0};
  // TODO more stats: # disk IO operations, # compactions, etc
};

struct BenchmarkOptions {
  // If true, write metrics to a CSV file, {metrics_path}/{metrics_filename}.csv
  // The exact metrics will depend on the benchmark. Note that metrics may
  // influence the performance of some benchmarks (since collecting the metrics
  // is extra overhead).
  bool write_metrics{false};
  std::string metrics_filename{"metrics"};
  std::filesystem::path metrics_path{"./metrics"};

  // If true, check that the results the DB is producing is actually correct.
  // For instance, WriteRandomBenchmark Put()'s a bunch of keys; if this flag
  // is true, it will make sure that Get()-ing those keys returns the expected
  // results. Turing this on will make the benchmarks take longer, and it should
  // be used for debugging/development only. Verification time is NOT included
  // in the benchmark runtime stats.
  bool verify_results{false};
};

class Benchmark {
 public:
  Benchmark(BenchmarkOptions options) : options_{options} {}

  Benchmark(const Benchmark&) = default;
  Benchmark& operator=(const Benchmark&) = default;

  Benchmark(Benchmark&&) = default;
  Benchmark& operator=(Benchmark&&) = default;

  virtual ~Benchmark() = default;

  // Return true on success, false on failure.
  virtual bool Run() = 0;

  // Get the stats from the last run
  BenchmarkStats GetStats() const noexcept { return stats_; }

 protected:
  BenchmarkStats stats_;
  BenchmarkOptions options_;
};

class WriteRandomBenchmark : public Benchmark {
 public:
  using Benchmark::Benchmark;

  // Default options for this benchmark
  static BenchmarkOptions GetBenchmarkOptions() {
    return {.write_metrics = true, .metrics_filename = "write_random_metrics"};
  }

  bool Run() override;
};

}  // namespace benchmark
}  // namespace mdb
