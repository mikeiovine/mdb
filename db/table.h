#pragma once

#include <string>
#include <map>
#include <shared_mutex>
#include <future>
#include <list>

#include "types.h"
#include "options.h"

namespace mdb {

class TableReader;

class Table {
    public:
        using LevelT = std::list<std::unique_ptr<TableReader>>;

        std::string ValueOf(std::string_view key) const;
        
        void WriteMemtable(
            const Options& options,
            const MemTableT& memtable);

    private:
        bool NeedsCompaction(int level);
        void Compact(int level, const Options& options);
        size_t TotalSize(int level);
        void WaitForOnGoingCompactions();

        void WriteMemtableInternal(
            int level, 
            const Options& options, 
            const MemTableT& memtable, 
            bool async);

        int next_table_{ 0 };

        std::map<int, LevelT> levels_;

        std::future<void> compaction_future_;

        mutable std::shared_mutex reader_mutex_;
};

}
