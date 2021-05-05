#include "disk_storage_manager.h"

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

DiskStorageManager::~DiskStorageManager() { WaitForOngoingCompactions(); }

std::string DiskStorageManager::ValueOf(std::string_view key) const {
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

void DiskStorageManager::WriteMemtable(const Options& options,
                                       const MemTableT& memtable) {
  std::unique_lock level_lk(level_mutex_);

  levels_[0].push_front(
      options.table_factory->TableFromMemtable(next_table_, options, memtable));

  ++next_table_;
  level_lk.unlock();

  std::scoped_lock compaction_lk(compaction_mutex_);
  if (!ongoing_compaction_ && NeedsCompaction(0, options)) {
    ongoing_compaction_ = true;
    std::thread(&DiskStorageManager::TriggerCompaction, this, 0, options)
        .detach();
  }
}

void DiskStorageManager::WaitForOngoingCompactions() {
  std::unique_lock lk(compaction_mutex_);
  compaction_cv_.wait(lk, [this] { return !ongoing_compaction_; });
}

bool DiskStorageManager::NeedsCompaction(int level, const Options& opt) const {
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

size_t DiskStorageManager::TotalSize(int level) const {
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

void DiskStorageManager::TriggerCompaction(int level, const Options& options) {
  Compact(level, options);
  std::scoped_lock compaction_lk(compaction_mutex_);
  ongoing_compaction_ = false;

  // Note that we do not unlock before notifying. This is due to a potential
  // edge case when destroying this object. The following (admittedly unlikely)
  // sequence of events would be possible if we released the mutex before
  // notifying: 1) ongoing_compaction = false, compaction_mutex_ unlocked 2) The
  // thread trying to destruct this DiskStorageManager spuriously wakes up. 3)
  // compaction_cv_.notify_all() is called in this tread; but compaction_cv_ is
  //    already destructed, so we get UB!
  compaction_cv_.notify_all();
}

void DiskStorageManager::Compact(int level, const Options& options) {
  std::shared_lock level_read_lock(level_mutex_);

  // levels_[level] should have already been created if we are calling
  // Compact(). Throw an exception if it doesn't exist.
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
  level_read_lock.unlock();

  std::unique_lock level_write_lock(level_mutex_);
  auto table_id{next_table_};
  ++next_table_;
  level_write_lock.unlock();

  auto output_io{options.table_factory->MakeTableWriter(table_id, options)};

  std::string last_key{""};

  while (!pq.empty()) {
    auto next_pair{pq.top()};

    // If we've seen the key before, we don't want to take it.
    if (next_pair.kv.first != last_key) {
      // Don't take deleted keys during compaction.
      if (!next_pair.kv.second.empty()) {
        output_io->Add(next_pair.kv.first, next_pair.kv.second);
      }
      last_key = std::move(next_pair.kv.first);
    }

    std::pair<TableIterator, TableIterator>& it{
        iterators[next_pair.iterator_id]};
    ++it.first;

    pq.pop();
    if (it.first != it.second) {
      pq.emplace(*it.first, next_pair.iterator_id);
    }
  }

  if (output_io->NumKeys() > 0) {
    output_io->Flush();

    level_write_lock.lock();
    levels_[level + 1].push_front(
        options.table_factory->MakeTableReader(*output_io, options));
    level_write_lock.unlock();

    if (NeedsCompaction(level + 1, options)) {
      Compact(level + 1, options);
    }

  } else {
    options.env->RemoveFile(output_io->GetFileName());
  }

  level_write_lock.lock();
  for (auto it = level_list_start; it != level_list.end(); it++) {
    options.env->RemoveFile((*it)->GetFileName());
  }

  level_list.erase(level_list_start, level_list.end());
}

}  // namespace mdb
