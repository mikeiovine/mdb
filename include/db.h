#pragma once

#include "options.h"
#include "memtable.h"
#include "log_writer.h"

#include <string>
#include <mutex>
#include <list>
#include <shared_mutex>

namespace mdb {

class DB {
    public:
        DB(Options options);

        void Put(const std::string& key, const std::string& value);
        
        std::string Get(const std::string& key);

    private:
        void LogWrite(const std::string& key, const std::string& value);
        void UpdateMemtable(const std::string& key, const std::string& value);
        void WriteMemtable();

        Options options_;

        LogWriter logger_;

        std::mutex write_mutex_;
        std::shared_mutex memtable_mutex_;

        int next_log_{ 1 };
        int next_table_{ 0 };

        size_t cache_size_{ 0 };

        MemTableT memtable_;

        std::list<std::unique_ptr<TableReader>> readers_;
};

}
