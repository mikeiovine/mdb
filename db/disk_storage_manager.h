#pragma once

#include <future>
#include <list>
#include <map>
#include <queue>
#include <shared_mutex>
#include <string>

#include "options.h"
#include "table_reader.h"
#include "types.h"

namespace mdb {

class DiskStorageManager {
 public:
  using LevelT = std::list<std::unique_ptr<TableReader>>;

  DiskStorageManager() = default;

  DiskStorageManager(const DiskStorageManager&) = delete;
  DiskStorageManager& operator=(const DiskStorageManager&) = delete;

  DiskStorageManager(DiskStorageManager&&) = delete;
  DiskStorageManager& operator=(DiskStorageManager&&) = delete;

  ~DiskStorageManager();

  // Concurrent calls to ValueOf/WriteMemtable are safe.
  std::string ValueOf(std::string_view key) const;

  // This method requires external synchronization. The implementation
  // assumes that it is being called by only one thread.
  void WriteMemtable(const Options& options, const MemTableT& memtable);

  void WaitForOngoingCompactions();

  // Load the specified tables into the the system. Assumes higher table
  // numbers are more recent.
  //
  // This function is technically thread-safe, but it's not really intended to
  // be used concurrently with other operations. It's meant for loading the
  // tables during database construction in recovery mode.
  void LoadIndices(std::priority_queue<size_t>& table_numbers,
                   const Options& opt);

 private:
  bool NeedsCompaction(size_t level, const Options& options) const;
  void Compact(size_t level, const Options& options);
  void TriggerCompaction(size_t level, const Options& options);
  size_t TotalSize(size_t level) const;

  size_t next_table_{0};

  std::map<size_t, LevelT> levels_;

  mutable std::shared_mutex level_mutex_;

  std::mutex compaction_mutex_;
  std::condition_variable compaction_cv_;
  bool ongoing_compaction_{false};
};

}  // namespace mdb
