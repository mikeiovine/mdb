#pragma once

#include "table_writer.h"
#include "table_reader.h"
#include "options.h"

namespace mdb {

template <class MemTableT>
std::unique_ptr<TableReader> UncompressedTableFactory<MemTableT>::MakeTable(
    const std::string& filename,
    const MDBOptions& options,
    const MemTableT& memtable) {

    UncompressedTableWriter<MemTableT> writer{
        options.env->MakeWriteOnlyIO(filename),
        options.write_sync,
        options.block_size
    };

    writer.WriteMemtable(memtable);

    return std::make_unique<UncompressedTableReader>(
        options.env->MakeReadOnlyIO(filename), 
        writer.GetIndex());
}

} // namespace mdb
