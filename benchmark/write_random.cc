#include <fstream>
#include <iostream>
#include <set>

#include "benchmark_interface.h"
#include "benchmark_util.h"
#include "db.h"

namespace mdb {
namespace benchmark {

bool WriteRandomBenchmark::Run() {
  // TODO make this directory automatically
  Options opt{.path = "./benchmark/db_files/write_random"};
  DB db(opt);

  auto metrics{OpenMetricsFile(options_, GetMetricsFilename())};
  if (metrics) {
    metrics.value() << "write_num,write_time(microseconds)\n";
  }

  auto key_value_pairs{CreateRandomKeyValuePairs(100, 16, 100)};

  auto start{std::chrono::high_resolution_clock::now()};
  int n{1};
  for (const auto &pair : key_value_pairs) {
    auto start_single{std::chrono::high_resolution_clock::now()};
    db.Put(pair.first, pair.second);
    auto end_single{std::chrono::high_resolution_clock::now()};

    if (options_.write_metrics && metrics) {
      auto diff{std::chrono::duration_cast<std::chrono::microseconds>(
          end_single - start_single)};
      metrics.value() << n << "," << diff.count() << '\n';
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
