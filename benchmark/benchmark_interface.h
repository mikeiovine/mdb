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
  std::filesystem::path metrics_path{"./metrics"};

  // If true, check that the results the DB is producing is actually correct.
  // For instance, WriteRandomBenchmark Put()'s a bunch of keys; if this flag
  // is true, it will make sure that Get()-ing those keys returns the expected
  // results. Turing this on will make the benchmarks take longer, and it should
  // be used for debugging/development only. Verification time is NOT included
  // in the benchmark runtime stats.
  bool verify_results{false};

  int64_t key_size{0};

  int64_t value_size{0};

  int64_t num_entries{0};
};

class Benchmark {
 public:
  Benchmark(BenchmarkOptions options) : options_{std::move(options)} {}

  Benchmark(const Benchmark&) = default;
  Benchmark& operator=(const Benchmark&) = default;

  Benchmark(Benchmark&&) = default;
  Benchmark& operator=(Benchmark&&) = default;

  virtual ~Benchmark() = default;

  // Return true on success, false on failure.
  virtual bool Run() = 0;

  // Get the stats from the last run
  BenchmarkStats GetStats() const noexcept { return stats_; }

  // Get the name of the file to write metrics to
  virtual std::string GetMetricsFilename() const = 0;

 protected:
  BenchmarkStats stats_;
  BenchmarkOptions options_;
};

class WriteRandomBenchmark : public Benchmark {
 public:
  WriteRandomBenchmark(BenchmarkOptions options)
      : Benchmark(std::move(options)) {}

  bool Run() override;

  std::string GetMetricsFilename() const override { return "write_random.csv"; }
};

class ReadRandomBenchmark : public Benchmark {
 public:
  ReadRandomBenchmark(BenchmarkOptions options)
      : Benchmark(std::move(options)) {}

  bool Run() override;

  std::string GetMetricsFilename() const override { return "read_random.csv"; }
};

}  // namespace benchmark
}  // namespace mdb
