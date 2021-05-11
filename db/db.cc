#include "db.h"

#include <boost/log/trivial.hpp>
#include <iostream>
#include <queue>

#include "helpers.h"
#include "log_reader.h"

namespace mdb {

DB::DB(Options opt) : options_{std::move(opt)} {
  if (!std::filesystem::is_directory(options_.path)) {
    BOOST_LOG_TRIVIAL(info) << "Specified path not found. Trying to create.";
    std::filesystem::create_directories(options_.path);
  }

  if (!options_.recovery_mode) {
    BOOST_LOG_TRIVIAL(info)
        << "Recovery mode is off. Any existing DB files will be removed.";
    for (auto& p : std::filesystem::directory_iterator(options_.path)) {
      std::filesystem::remove_all(p);
    }
  }

  BOOST_LOG_TRIVIAL(info) << "Opening mdb at "
                          << std::filesystem::canonical(options_.path);

  if (options_.recovery_mode) {
    Recover();
  } else {
    InitNextLogWriter();
  }
}

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

  return disk_storage_manager_.ValueOf(key);
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
    BOOST_LOG_TRIVIAL(info) << "Flushing memtable to disk.";
    // Flush the memtable and create a new reader.
    disk_storage_manager_.WriteMemtable(options_, memtable_);

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
    BOOST_LOG_TRIVIAL(error)
        << "Failed to remove obsolete log file " << logger_.GetFileName();
  }

  InitNextLogWriter();

  memtable_.clear();
}

void DB::WaitForOngoingCompactions() {
  disk_storage_manager_.WaitForOngoingCompactions();
}

void DB::Recover() {
  // Priority queue since we want to grab the max eventually.
  std::vector<size_t> log_file_indices;
  std::priority_queue<size_t> table_file_indices;

  for (const auto& file : std::filesystem::directory_iterator(options_.path)) {
    auto file_info{util::GetFileInfo(file)};
    switch (file_info.id) {
      case util::FileType::LogFile:
        log_file_indices.push_back(file_info.index);
        break;
      case util::FileType::TableFile:
        table_file_indices.push(file_info.index);
        break;
      case util::FileType::Unknown:
        std::error_code ec;
        auto success = std::filesystem::remove_all(file, ec);
        if (!success) {
          BOOST_LOG_TRIVIAL(warning)
              << "Could not remove extraneous file " << file.path().string();
        }
        break;
    }
  }

  LoadLogFile(log_file_indices);
  disk_storage_manager_.LoadIndices(table_file_indices, options_);
}

void DB::LoadLogFile(const std::vector<size_t>& log_file_indices) {
  if (log_file_indices.empty()) {
    BOOST_LOG_TRIVIAL(warning)
        << "DB was started in recovery mode, but no log file was found.";
  } else {
    next_log_ =
        *std::max_element(log_file_indices.begin(), log_file_indices.end());
    LogReader reader{next_log_, options_};
    memtable_ = reader.ReadMemTable();

    for (const auto& idx : log_file_indices) {
      if (idx != next_log_) {
        auto fname{util::LogFileName(options_, idx)};
        try {
          options_.env->RemoveFile(fname);
        } catch (const std::system_error&) {
          BOOST_LOG_TRIVIAL(error)
              << "Failed to remove obsolete log file " << fname;
        }
      }
    }
  }

  InitNextLogWriter();
}

void DB::InitNextLogWriter() {
  logger_ = LogWriter(next_log_, options_);
  next_log_ += 1;
}

}  // namespace mdb
