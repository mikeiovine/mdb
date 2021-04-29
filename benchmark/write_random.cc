#include <gflags/gflags.h>

#include <set>

#include "benchmark_interface.h"
#include "db.h"

using namespace mdb;

DECLARE_uint32(num_entries);
DECLARE_uint32(key_size);
DECLARE_uint32(value_size);

namespace {

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

std::vector<std::pair<std::string, std::string>> CreateKeyValuePairs() {
  std::vector<std::pair<std::string, std::string>> key_values;
  for (size_t i = 0; i < FLAGS_num_entries; i++) {
    key_values.emplace_back(RandomString(FLAGS_key_size),
                            RandomString(FLAGS_value_size));
  }
  return key_values;
}

}  // namespace

bool WriteRandomBenchmark::Run() {
  // TODO make this directory automatically
  Options opt{.path = "./benchmark/db_files/write_random"};
  DB db(opt);

  auto key_value_pairs{CreateKeyValuePairs()};

  auto start{std::chrono::steady_clock::now()};
  for (const auto& pair : key_value_pairs) {
    db.Put(pair.first, pair.second);
  }
  auto end{std::chrono::steady_clock::now()};
  stats_.time = std::chrono::duration_cast<std::chrono::seconds>(end - start);

  std::set<std::string> seen_keys;
  for (auto it = key_value_pairs.crbegin(); it != key_value_pairs.crend();
       it++) {
    // Only consider the most recent version of duplicate keys
    if (seen_keys.find(it->first) == seen_keys.end()) {
      if (!(db.Get(it->first) == it->second)) {
        stats_.failure_reason = "Database wrote wrong value '" + it->second +
                                "' for key '" + it->first + "'";

        return false;
      }
      seen_keys.insert(it->first);
    }
  }

  return true;
}

std::shared_ptr<Benchmark> WriteRandomBenchmark::Create() const {
  return std::make_shared<WriteRandomBenchmark>();
}
