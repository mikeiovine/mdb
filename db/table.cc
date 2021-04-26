#include "table.h"

#include <chrono>
#include <cmath>
#include <vector>

#include "iterator.h"
#include "table_reader.h"

namespace mdb {

std::string Table::ValueOf(std::string_view key) const {
  std::shared_lock lk(level_mutex_);

  for (const auto& levelid_and_level : levels_) {
    for (const auto& reader : levelid_and_level.second) {
      // This string is possibly empty if the table has
      // the key marked as deleted.
      auto val{reader->ValueOf(key)};
      if (val) {
        return val.value();
      }
    }
  }

  return "";
}

void Table::WriteMemtable(const Options& options, const MemTableT& memtable) {
  WaitForOnGoingCompactions();
  WriteMemtableInternal(0, options, memtable, true);
}

void Table::WriteMemtableInternal(int level, const Options& options,
                                  const MemTableT& memtable, bool async) {
  std::unique_lock lk(level_mutex_);

  bool write_deleted{level == 0};
  levels_[level].push_front(options.table_factory->TableFromMemtable(
      next_table_, options, memtable, write_deleted));

  ++next_table_;
  lk.unlock();

  if (NeedsCompaction(level)) {
    if (async) {
      compaction_future_ = std::async(&Table::Compact, this, level, options);
    } else {
      Compact(level, options);
    }
  }
}

void Table::WaitForOnGoingCompactions() {
  using namespace std::chrono_literals;
  if (compaction_future_.valid()) {
    compaction_future_.get();
  }
}

bool Table::NeedsCompaction(int level) {
  if (level == 0) {
    return levels_[level].size() > 4;
  }

  return TotalSize(level) > std::pow(10, level + 1) * 1000 * 1000;
}

size_t Table::TotalSize(int level) {
  size_t total{0};

  for (const auto& reader : levels_[level]) {
    total += reader->Size();
  }

  return total;
}

void Table::Compact(int level, const Options& options) {
  LevelT& level_list{levels_[level]};

  std::vector<std::pair<TableIterator, TableIterator>> iterators;
  iterators.reserve(level_list.size());

  std::for_each(level_list.begin(), level_list.end(),
                [&iterators](const auto& it) {
                  iterators.emplace_back(it->Begin(), it->End());
                });

  // TODO need to figure out if it's OK to keep this in memory.
  // We might want to write to the file directly.
  MemTableT memtable_temp;

  auto is_done = [](const auto& iterators) {
    return std::all_of(iterators.begin(), iterators.end(),
                       [](const auto& begin_end) {
                         return begin_end.first == begin_end.second;
                       });
  };

  auto determine_kv = [](auto& iterators) {
    for (auto& begin_end : iterators) {
      if (begin_end.first != begin_end.second) {
        ++begin_end.first;
        return *begin_end.first;
      }
    }

    assert(false);
    // Not reached
    return std::make_pair<std::string, std::string>("", "");
  };

  while (!is_done(iterators)) {
    // Note: the insertion fails if the key is already in the memtable.
    // This is what we want! (the most recent key gets priority)
    memtable_temp.insert(determine_kv(iterators));
  }

  WriteMemtableInternal(level + 1, options, memtable_temp, false);

  std::unique_lock lk(level_mutex_);
  for (const auto& reader : level_list) {
    options.env->RemoveFile(reader->GetFileName());
  }
  level_list.clear();
}

}  // namespace mdb
