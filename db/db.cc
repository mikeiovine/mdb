#include "db.h"

namespace mdb {

DB::DB(Options opt) : 
    options_{ std::move(opt) },
    logger_{ LogWriter(0, options_) } {}

void DB::Put(const std::string& key, const std::string& value) {
    std::unique_lock<std::mutex> lk(write_mutex_);
    LogWrite(key, value);
    UpdateMemtable(key, value);
}

std::string DB::Get(const std::string& key) {
    // TODO
    return "";
}

void DB::LogWrite(const std::string& key, const std::string& value) {
    if (value.size() > 0) {
        logger_.Add(key, value);
    } else {
        logger_.MarkDelete(key);
    }
    
    if (logger_.Size() > options_.log_max_size) {
        logger_ = LogWriter(next_log_, options_);
        next_log_ += 1;
    }
}

void DB::UpdateMemtable(const std::string& key, const std::string& value) {
    if (value.size() > 0) {
        memtable_[key] = value;
        cache_size_ += key.size() + value.size();
    } else {
        auto pos{ memtable_.find(key) };
        if (pos != memtable_.end()) {
            memtable_.erase(pos);
        }
    }

    if (cache_size_ > options_.memtable_max_size) {
        WriteMemtable(); 
    }
}

void DB::WriteMemtable() {
    readers_.push_back(
        options_.table_factory->MakeTable(
            next_table_,
            options_,
            memtable_));

    cache_size_ = 0;
    next_table_ += 1;
}

}
