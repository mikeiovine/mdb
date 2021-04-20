#pragma once

#include <memory>

#include "table_reader.h"
#include "env.h"
#include "options.h"
#include "memtable.h"

namespace mdb {

class TableFactory {
    public:
        virtual ~TableFactory() = default;

        virtual std::unique_ptr<TableReader> MakeTable(
            const std::string& filename,
            const Options& options,
            const MemTableT& memtable) = 0;
};

class UncompressedTableFactory : public TableFactory {
    public:
        std::unique_ptr<TableReader> MakeTable(
            const std::string& filename,
            const Options& options,
            const MemTableT& memtable) override;
};

} // namespace mdb
