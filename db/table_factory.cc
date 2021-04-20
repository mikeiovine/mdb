#include "table_factory.h"
#include "table_writer.h"

namespace mdb {

std::unique_ptr<TableReader> UncompressedTableFactory::MakeTable(
    const std::string& filename,
    const Options& options,
    const MemTableT& memtable) {

    UncompressedTableWriter writer{
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
