#pragma once

#include <string>
#include <map>

#include "file.h"

namespace mdb {

class TableReader {
    public:
        virtual ~TableReader() = default;

        virtual std::string ValueOf(const std::string& key) = 0;
};

class UncompressedTableReader : public TableReader {
    public:
        UncompressedTableReader(
            std::unique_ptr<ReadOnlyIO> file,
            std::map<std::string, size_t> index) :
            file_{ std::move(file) },
            index_{ std::move(index) } {}

        std::string ValueOf(const std::string& key) override;

    private:
        std::string SearchInBlock(size_t block_loc, const std::string& key_to_find);

        std::unique_ptr<ReadOnlyIO> file_;
        const std::map<std::string, size_t> index_;
};

}
