#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "file.h"
#include "helpers.h"

namespace mdb {

template <class MemTableT>
class TableWriter {
    public:
       virtual ~TableWriter() = default;
       
       virtual void WriteMemtable(const MemTableT& memtable) = 0;

       virtual std::map<std::string, size_t> GetIndex() = 0;
};


template <class MemTableT>
class UncompressedTableWriter : public TableWriter<MemTableT> {
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

        std::map<std::string, size_t> GetIndex() override;

    private:
        std::vector<char> buf_;

        void Add(const std::string& key, const std::string& value);
        void Flush();

        std::unique_ptr<WriteOnlyIO> file_;
        std::map<std::string, size_t> index_; 

        const bool sync_;
        const size_t block_size_;

        size_t cur_index_{ 0 };
};

} // namespace mdb

#include "table_writer-inl.h"
