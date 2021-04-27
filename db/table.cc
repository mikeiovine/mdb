#include "table.h"

#include <chrono>
#include <cmath>
#include <queue>
#include <vector>

#include "iterator.h"
#include "table_reader.h"
#include "table_writer.h"

namespace mdb {

namespace {

struct KeyValue {
  KeyValue(std::pair<std::string, std::string> kv_, size_t iterator_id_)
      : kv{std::move(kv_)}, iterator_id{iterator_id_} {}

  friend bool operator<(const KeyValue& lhs, const KeyValue& rhs) {
    return lhs.kv.first > rhs.kv.first;
  }

  std::pair<std::string, std::string> kv;
  size_t iterator_id;
};

using PriorityQueue = std::priority_queue<KeyValue>;

}  // namespace

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
  WriteMemtableInternal(0, options, memtable);
}

void Table::WriteMemtableInternal(int level, const Options& options,
                                  const MemTableT& memtable) {
  std::unique_lock lk(level_mutex_);

  bool write_deleted{level == 0};
  levels_[level].push_front(options.table_factory->TableFromMemtable(
      next_table_, options, memtable, write_deleted));

  ++next_table_;
  lk.unlock();

  if (NeedsCompaction(level)) {
    compaction_future_ = std::async(&Table::Compact, this, level, options);
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

  PriorityQueue pq;

  std::vector<std::pair<TableIterator, TableIterator>> iterators;
  iterators.reserve(level_list.size());

  size_t iterator_id{0};
  for (const auto& reader : level_list) {
    if (reader->Begin() != reader->End()) {
      pq.emplace(*reader->Begin(), iterator_id);
      iterators.emplace_back(reader->Begin(), reader->End());
      ++iterator_id;
    }
  }

  std::string last_key{""};

  std::unique_lock lk(level_mutex_);
  auto output_io{options.table_factory->MakeTableWriter(next_table_, options)};
  ++next_table_;
  lk.unlock();

  while (!pq.empty()) {
    auto best{pq.top()};

    // If we've seen the key before, we don't want to take it.
    if (best.kv.first != last_key) {
      // Don't take deleted keys during compaction.
      if (!best.kv.second.empty()) {
        output_io->Add(best.kv.first, best.kv.second);
      }
      last_key = std::move(best.kv.first);
    }

    std::pair<TableIterator, TableIterator>& it{iterators[best.iterator_id]};
    ++it.first;

    if (it.first != it.second) {
      pq.emplace(*it.first, best.iterator_id);
    }
    pq.pop();
  }

  lk.lock();

  if (output_io->NumKeys() > 0) {
    output_io->Flush();
    levels_[level + 1].push_front(
        options.table_factory->MakeTableReader(*output_io, options));
    if (NeedsCompaction(level + 1)) {
      lk.unlock();
      Compact(level + 1, options);
      lk.lock();
    }

  } else {
    options.env->RemoveFile(output_io->GetFileName());
  }

  for (const auto& reader : level_list) {
    options.env->RemoveFile(reader->GetFileName());
  }
  level_list.clear();
}

}  // namespace mdb
