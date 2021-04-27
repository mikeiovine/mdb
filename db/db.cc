#include "db.h"

namespace mdb {

DB::DB(Options opt)
    : options_{std::move(opt)}, logger_{LogWriter(0, options_)} {}

DB::~DB() { table_.WaitForOnGoingCompactions(); }

void DB::Put(std::string_view key, std::string_view value) {
  if (key.empty() || value.empty()) {
    throw std::invalid_argument("Key and value must be non-empty.");
  }

  PutOrDelete(key, value);
}

std::string DB::Get(std::string_view key) {
  std::shared_lock lk(memtable_mutex_);

  auto value_loc{memtable_.find(key)};
  if (value_loc != memtable_.end()) {
    return value_loc->second;
  }

  lk.unlock();

  return table_.ValueOf(key);
}

void DB::Delete(std::string_view key) {
  if (key.empty()) {
    throw std::invalid_argument("Key must be non-empty.");
  }

  PutOrDelete(key, "");
}

void DB::PutOrDelete(std::string_view key, std::string_view value) {
  std::unique_lock<std::mutex> lk(write_mutex_);

  logger_.Add(key, value);

  std::unique_lock memtable_lk(memtable_mutex_);
  UpdateMemtable(key, value);
  memtable_lk.unlock();

  if (cache_size_ > options_.memtable_max_size) {
    // Flush the memtable and create a new reader.
    table_.WriteMemtable(options_, memtable_);

    memtable_lk.lock();
    ClearMemtable();
  }
}

void DB::UpdateMemtable(std::string_view key, std::string_view value) {
  memtable_.insert_or_assign(std::string(key), value);
  cache_size_ += key.size() + value.size();
}

void DB::ClearMemtable() {
  cache_size_ = 0;

  // Now that the memtable has been written, the logfile can
  // be removed.
  try {
    options_.env->RemoveFile(logger_.GetFileName());
  } catch (const std::system_error&) {
    // TODO log this error.
  }

  logger_ = LogWriter(next_log_, options_);
  next_log_ += 1;
}

void DB::WaitForOnGoingCompactions() { table_.WaitForOnGoingCompactions(); }

}  // namespace mdb
