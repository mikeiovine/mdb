#pragma once

#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "log_writer.h"
#include "options.h"
#include "table.h"
#include "types.h"

namespace mdb {

class DB {
 public:
  DB(Options options);

  void Put(std::string_view key, std::string_view value);

  std::string Get(std::string_view key);

  void Delete(std::string_view key);

  void WaitForOngoingCompactions();

 private:
  void PutOrDelete(std::string_view key, std::string_view value);
  void UpdateMemtable(std::string_view key, std::string_view value);
  void ClearMemtable();

  Options options_;

  LogWriter logger_;

  std::mutex write_mutex_;
  std::shared_mutex memtable_mutex_;

  int next_log_{1};
  size_t cache_size_{0};

  MemTableT memtable_;

  Table table_;
};

}  // namespace mdb
