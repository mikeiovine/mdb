#pragma once

#include <chrono>
#include <memory>
#include <string>

struct BenchmarkStats {
  std::string failure_reason{""};

  std::chrono::seconds time{0};
  // TODO more stats: # disk IO operations, # compactions, etc
};

class Benchmark {
 public:
  Benchmark() = default;

  Benchmark(const Benchmark&) = default;
  Benchmark& operator=(const Benchmark&) = default;

  Benchmark(Benchmark&&) = default;
  Benchmark& operator=(Benchmark&&) = default;

  virtual ~Benchmark() = default;

  // Return true on success, false on failure.
  virtual bool Run() = 0;

  // Get the stats from the last run
  BenchmarkStats GetStats() const noexcept { return stats_; }

  // Factory method to create a new benchmark from a prototypical instance.
  virtual std::shared_ptr<Benchmark> Create() const = 0;

 protected:
  BenchmarkStats stats_;
};

class WriteRandomBenchmark : public Benchmark {
 public:
  bool Run() override;
  std::shared_ptr<Benchmark> Create() const override;
};
