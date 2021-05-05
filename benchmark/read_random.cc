#include <gflags/gflags.h>

#include "benchmark_interface.h"
#include "benchmark_util.h"
#include "db.h"

namespace mdb {
namespace benchmark {

DECLARE_uint32(num_entries);
DECLARE_uint32(key_size);
DECLARE_uint32(value_size);

bool ReadRandomBenchmark::Run() {
  Options opt{.path = "./benchmark/db_files/read_random"};
  DB db(opt);

  auto metrics{OpenMetricsFile(options_)};
  if (metrics) {
    metrics.value() << "read_num,read_time(microseconds)\n";
  }

  auto key_value_pairs{CreateRandomKeyValuePairs(
      FLAGS_num_entries, FLAGS_key_size, FLAGS_value_size)};

  for (const auto& pair : key_value_pairs) {
    db.Put(pair.first, pair.second);
  }

  db.WaitForOngoingCompactions();

  int n{1};
  auto start{std::chrono::high_resolution_clock::now()};
  for (const auto& pair : key_value_pairs) {
    auto start_single{std::chrono::high_resolution_clock::now()};
    auto value{db.Get(pair.first)};
    auto end_single{std::chrono::high_resolution_clock::now()};

    if (options_.write_metrics && metrics) {
      auto diff{std::chrono::duration_cast<std::chrono::microseconds>(
          end_single - start_single)};
      metrics.value() << n << "," << diff.count() << '\n';
      ++n;
    }

    if (options_.verify_results && value != pair.second) {
      stats_.failure_reason = "Got wrong value '" + value + "' for key '" +
                              pair.first + "'. Expected: '" + pair.second + "'";
      return false;
    }
  }
  auto end{std::chrono::high_resolution_clock::now()};
  stats_.time = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  return true;
}

}  // namespace benchmark
}  // namespace mdb
