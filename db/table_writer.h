#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "file.h"
#include "helpers.h"
#include "types.h"

namespace mdb {

class TableWriter {
    public:
       virtual ~TableWriter() = default;
       
       virtual void WriteMemtable(const MemTableT& memtable) = 0;

       virtual IndexT GetIndex() = 0;
};


class UncompressedTableWriter : public TableWriter {
    public:
        UncompressedTableWriter(
            std::unique_ptr<WriteOnlyIO> file,
            bool sync,
            size_t block_size) :
            file_{ std::move(file) },
            sync_{ sync },
            block_size_{ block_size } {
            assert(file_ != nullptr);
        }

        void WriteMemtable(const MemTableT& memtable) override;

        IndexT GetIndex() override;

    private:
        std::vector<char> buf_;

        void Add(std::string_view key, std::string_view value);
        void Flush();

        std::unique_ptr<WriteOnlyIO> file_;
        IndexT index_; 

        const bool sync_;
        const size_t block_size_;

        size_t cur_index_{ 0 };
};

} // namespace mdb
