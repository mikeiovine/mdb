#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <set>

#include "benchmark_interface.h"
#include "benchmark_util.h"
#include "db.h"

namespace mdb {
namespace benchmark {

DECLARE_uint32(num_entries);
DECLARE_uint32(key_size);
DECLARE_uint32(value_size);

bool WriteRandomBenchmark::Run() {
  // TODO make this directory automatically
  Options opt{.path = "./benchmark/db_files/write_random"};
  DB db(opt);

  auto filename{options_.metrics_path / (options_.metrics_filename + ".csv")};
  std::ofstream metrics{filename};

  if (options_.write_metrics) {
    if (!metrics.is_open()) {
      std::cerr << "WARNING: write_metrics is set to true, but the file "
                << filename
                << " failed to open. Metrics will not be written.\n";
    } else {
      metrics << "write_num,time (microsec)\n";
    }
  }

  auto key_value_pairs{CreateRandomKeyValuePairs(
      FLAGS_num_entries, FLAGS_key_size, FLAGS_value_size)};

  auto start{std::chrono::high_resolution_clock::now()};
  int n{1};
  for (const auto &pair : key_value_pairs) {
    auto start_single{std::chrono::high_resolution_clock::now()};
    db.Put(pair.first, pair.second);
    auto end_single{std::chrono::high_resolution_clock::now()};

    if (options_.write_metrics) {
      auto diff{std::chrono::duration_cast<std::chrono::microseconds>(
          end_single - start_single)};
      metrics << n << "," << diff.count() << '\n';
      ++n;
    }
  }

  db.WaitForOngoingCompactions();

  auto end{std::chrono::high_resolution_clock::now()};
  stats_.time = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  if (options_.verify_results) {
    std::set<std::string> seen_keys;
    for (auto it = key_value_pairs.crbegin(); it != key_value_pairs.crend();
         it++) {
      // Only consider the most recent version of duplicate keys
      if (seen_keys.find(it->first) == seen_keys.end()) {
        auto val{db.Get(it->first)};
        if (val != it->second) {
          if (val.empty()) {
            val = "[empty string]";
          }

          stats_.failure_reason = "Database wrote wrong value '" + val +
                                  "' for key '" + it->first + "'. Expected: '" +
                                  it->second + "'";

          return false;
        }
        seen_keys.insert(it->first);
      }
    }
  }

  return true;
}

}  // namespace benchmark
}  // namespace mdb
