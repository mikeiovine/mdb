#include "table_factory.h"
#include "table_writer.h"
#include "options.h"

namespace mdb {

std::unique_ptr<TableReader> UncompressedTableFactory::MakeTable(
    int table_number,
    const Options& options,
    const MemTableT& memtable) {

    UncompressedTableWriter writer{
        options.env->MakeWriteOnlyIO(
            util::TableFileName(options, table_number)
        ),
        options.write_sync,
        options.block_size
    };

    writer.WriteMemtable(memtable);

    return std::make_unique<UncompressedTableReader>(
        options.env->MakeReadOnlyIO(
            util::TableFileName(options, table_number)
        ),
        writer.GetIndex());
}

} // namespace mdb
