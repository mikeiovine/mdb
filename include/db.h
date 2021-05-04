#pragma once

#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "disk_storage_manager.h"
#include "log_writer.h"
#include "options.h"
#include "types.h"

namespace mdb {

class DB {
 public:
  DB(Options options);

  void Put(std::string_view key, std::string_view value);

  std::string Get(std::string_view key);

  void Delete(std::string_view key);

  // Concurrent calls to WaitForOngoingCompaction and the other public
  // methods are safe. However, be aware that if a writer thread A
  // calls Put() at the same time that thread B calls
  // WaitForOngoingCompactions(), and thread A triggers a compaction, then
  // thread B may or may not wait for the compaction depending on the exact
  // ordering of events. To avoid any surprises, use this method after all
  // writer threads finish their work.
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

  DiskStorageManager disk_storage_manager_;
};

}  // namespace mdb
