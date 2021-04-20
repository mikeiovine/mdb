#include "table_factory.h"
#include "table_writer.h"
#include "options.h"

namespace mdb {

std::unique_ptr<TableReader> UncompressedTableFactory::MakeTable(
    int table_number,
    const Options& options,
    const MemTableT& memtable) {

    std::string filename{ util::TableFileName(options, table_number) };

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
