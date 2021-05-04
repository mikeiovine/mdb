#pragma once

#include <future>
#include <list>
#include <map>
#include <shared_mutex>
#include <string>

#include "options.h"
#include "table_reader.h"
#include "types.h"

namespace mdb {

class Table {
 public:
  using LevelT = std::list<std::unique_ptr<TableReader>>;

  Table() = default;

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  Table(Table&&) = delete;
  Table& operator=(Table&&) = delete;

  ~Table();

  // Concurrent calls to ValueOf/WriteMemtable are safe.
  std::string ValueOf(std::string_view key) const;

  // This method requires external synchronization. The implementation
  // assumes that it is being called by only one thread.
  void WriteMemtable(const Options& options, const MemTableT& memtable);

  void WaitForOngoingCompactions();

 private:
  bool NeedsCompaction(int level, const Options& options) const;
  void Compact(int level, const Options& options);
  void TriggerCompaction(int level, const Options& options);
  size_t TotalSize(int level) const;
  void CreateLevelIfAbsent(int level);

  int next_table_{0};

  std::map<int, LevelT> levels_;

  mutable std::shared_mutex level_mutex_;

  std::mutex compaction_mutex_;
  std::condition_variable compaction_cv_;
  bool ongoing_compaction_{false};
};

}  // namespace mdb
