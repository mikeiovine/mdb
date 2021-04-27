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

  // Concurrent calls to ValueOf/WriteMemtable are safe.
  // However, multiple concurrent calls to WriteMemtable
  // are unsafe: don't try to trigger multiple compactions
  // at the same time!
  //
  // Additionally, WriteMemtable blocks if there is an
  // ongoing compaction.

  std::string ValueOf(std::string_view key) const;

  void WriteMemtable(const Options& options, const MemTableT& memtable);

  void WaitForOnGoingCompactions();

 private:
  bool NeedsCompaction(int level);
  void Compact(int level, const Options& options);
  void TriggerCompaction(int level, const Options& options);
  size_t TotalSize(int level);

  int next_table_{0};

  std::map<int, LevelT> levels_;

  std::future<void> compaction_future_;

  mutable std::shared_mutex level_mutex_;

  bool ongoing_compaction_{false};
};

}  // namespace mdb
