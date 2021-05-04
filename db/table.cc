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

  friend bool operator>(const KeyValue& lhs, const KeyValue& rhs) {
    int cmp{lhs.kv.first.compare(rhs.kv.first)};
    if (cmp == 0) {
      // In the case that the strings are equal, the more recent one is
      // smaller. This causes it to have higher priority in the min-heap.
      return lhs.iterator_id > rhs.iterator_id;
    }
    return cmp > 0;
  }

  std::pair<std::string, std::string> kv;

  // Lower ID = more recent.
  size_t iterator_id;
};

}  // namespace

using PriorityQueue = std::priority_queue<KeyValue, std::vector<KeyValue>,
                                          // Use a min-heap
                                          std::greater<KeyValue>>;

Table::~Table() { WaitForOngoingCompactions(); }

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
  std::unique_lock level_lk(level_mutex_);

  levels_[0].push_front(
      options.table_factory->TableFromMemtable(next_table_, options, memtable));

  ++next_table_;
  level_lk.unlock();

  std::scoped_lock compaction_lk(compaction_mutex_);
  if (!ongoing_compaction_ && NeedsCompaction(0, options)) {
    ongoing_compaction_ = true;
    std::thread(&Table::TriggerCompaction, this, 0, options).detach();
  }
}

void Table::WaitForOngoingCompactions() {
  std::unique_lock lk(compaction_mutex_);
  compaction_cv_.wait(lk, [this] { return !ongoing_compaction_; });
}

bool Table::NeedsCompaction(int level, const Options& opt) const {
  if (level == 0) {
    std::shared_lock lk(level_mutex_);
    auto it{levels_.find(0)};
    if (it == levels_.end()) {
      return false;
    }

    return it->second.size() >= opt.trigger_compaction_at;
  }

  return TotalSize(level) > std::pow(10, level + 1) * 1000 * 1000;
}

size_t Table::TotalSize(int level) const {
  size_t total{0};

  std::shared_lock lk(level_mutex_);
  auto it{levels_.find(level)};
  if (it == levels_.end()) {
    return 0;
  }

  for (const auto& reader : it->second) {
    total += reader->Size();
  }

  return total;
}

void Table::TriggerCompaction(int level, const Options& options) {
  Compact(level, options);
  {
    std::scoped_lock compaction_lk(compaction_mutex_);
    ongoing_compaction_ = false;
  }
  compaction_cv_.notify_all();
}

void Table::CreateLevelIfAbsent(int level) {
  std::scoped_lock lk(level_mutex_);
  levels_[level] = LevelT{};
}

void Table::Compact(int level, const Options& options) {
  CreateLevelIfAbsent(level);

  std::shared_lock level_read(level_mutex_);
  LevelT& level_list{levels_.at(level)};

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

  // Save this position because tables might be added while we're doing the
  // compaction. Note insertions won't invalidate level_list_start.
  auto level_list_start{level_list.begin()};
  level_read.unlock();

  std::unique_lock level_write(level_mutex_);
  auto table_id{next_table_};
  ++next_table_;
  level_write.unlock();

  auto output_io{options.table_factory->MakeTableWriter(table_id, options)};

  std::string last_key{""};

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

    pq.pop();
    if (it.first != it.second) {
      pq.emplace(*it.first, best.iterator_id);
    }
  }

  if (output_io->NumKeys() > 0) {
    output_io->Flush();

    level_write.lock();
    levels_[level + 1].push_front(
        options.table_factory->MakeTableReader(*output_io, options));
    level_write.unlock();

    if (NeedsCompaction(level + 1, options)) {
      Compact(level + 1, options);
    }

  } else {
    options.env->RemoveFile(output_io->GetFileName());
  }

  level_write.lock();
  for (auto it = level_list_start; it != level_list.end(); it++) {
    options.env->RemoveFile((*it)->GetFileName());
  }

  level_list.erase(level_list_start, level_list.end());
}

}  // namespace mdb
