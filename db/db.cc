#include "db.h"

namespace mdb {

DB::DB(Options opt) : 
    options_{ std::move(opt) },
    logger_{ LogWriter(0, options_) } {}

void DB::Put(std::string_view key, std::string_view value) {
    std::unique_lock<std::mutex> lk(write_mutex_);

    if (value.size() > 0) {
        logger_.Add(key, value);
    } else {
        logger_.MarkDelete(key);
    }
    
    std::unique_lock memtable_lk(memtable_mutex_);
    UpdateMemtable(key, value);

    if (cache_size_ > options_.memtable_max_size) {
        WriteMemtable(); 
        memtable_.clear();
    }
}

std::string DB::Get(std::string_view key) {
    std::shared_lock lk(memtable_mutex_);
    
    auto value_loc{ memtable_.find(key) };
    if (value_loc != memtable_.end()) {
        return value_loc->second;
    }

    memtable_mutex_.unlock();

    return table_.ValueOf(key);
}

void DB::UpdateMemtable(std::string_view key, std::string_view value) {
    // Copy the data in the views
    std::string value_str{ value };
    std::string key_str{ key };

    if (value_str.size() > 0) {
        memtable_[key_str] = value_str;
        cache_size_ += key_str.size() + value_str.size();
    } else {
        auto pos{ memtable_.find(key_str) };
        if (pos != memtable_.end()) {
            memtable_.erase(pos);
        }
    }
}

void DB::WriteMemtable() {
    // Flush the memtable and create a new reader.
    table_.WriteMemtable(options_, memtable_);
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

}
