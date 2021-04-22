#pragma once

#include <string>

#include "file.h"
#include "types.h"

namespace mdb {

class TableReader {
    public:
        TableReader() = default;

        TableReader(const TableReader&) = delete;
        TableReader& operator=(const TableReader&) = delete;

        TableReader(TableReader&&) = delete;
        TableReader& operator=(TableReader&&) = delete;

        virtual ~TableReader() = default;

        virtual std::string ValueOf(std::string_view key) = 0;
};

class UncompressedTableReader : public TableReader {
    public:
        UncompressedTableReader(
            std::unique_ptr<ReadOnlyIO> file,
            IndexT index) :
            file_{ std::move(file) },
            index_{ std::move(index) } {}

        std::string ValueOf(std::string_view key) override;

    private:
        std::string SearchInBlock(size_t block_loc, std::string_view key_to_find);

        std::unique_ptr<ReadOnlyIO> file_;
        const IndexT index_;
};

}
