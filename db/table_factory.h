#pragma once

#include <memory>

#include "table_reader.h"
#include "env.h"
#include "options.h"

namespace mdb {

template <class MemTableT>
class TableFactory {
    public:
        virtual ~TableFactory() = default;

        virtual std::unique_ptr<TableReader> MakeTable(
            const std::string& filename,
            const MDBOptions& options,
            const MemTableT& memtable) = 0;
};

template<class MemTableT>
class UncompressedTableFactory : public TableFactory<MemTableT> {
    public:
        std::unique_ptr<TableReader> MakeTable(
            const std::string& filename,
            const MDBOptions& options,
            const MemTableT& memtable) override;
};

} // namespace mdb

#include "table_factory-inl.h"
