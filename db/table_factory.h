#pragma once

#include <memory>

#include "table_reader.h"
#include "env.h"
#include "types.h"

namespace mdb {

struct Options;

class TableFactory {
    public:
        virtual ~TableFactory() = default;

        virtual std::unique_ptr<TableReader> MakeTable(
            int table_number,
            const Options& options,
            const MemTableT& memtable) = 0;
};

class UncompressedTableFactory : public TableFactory {
    public:
        std::unique_ptr<TableReader> MakeTable(
            int table_number,
            const Options& options,
            const MemTableT& memtable) override;
};

} // namespace mdb
