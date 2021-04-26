#include "table_factory.h"

#include "options.h"
#include "table_reader.h"
#include "table_writer.h"

namespace mdb {

std::unique_ptr<TableReader> UncompressedTableFactory::TableFromMemtable(
    int table_number, const Options& options, const MemTableT& memtable,
    bool write_deleted) {
  UncompressedTableWriter writer{
      options.env->MakeWriteOnlyIO(util::TableFileName(options, table_number)),
      options.write_sync, options.block_size};

  writer.WriteMemtable(memtable, write_deleted);

  return std::make_unique<UncompressedTableReader>(
      options.env->MakeReadOnlyIO(writer.GetFileName()), writer.GetIndex());
}

std::unique_ptr<TableWriter> UncompressedTableFactory::MakeTableWriter(
    int table_number, const Options& options) {
  return std::make_unique<UncompressedTableWriter>(
      options.env->MakeWriteOnlyIO(util::TableFileName(options, table_number)),
      options.write_sync, options.block_size);
}

std::unique_ptr<TableReader> UncompressedTableFactory::MakeTableReader(
    const TableWriter& writer, const Options& options) {
  return std::make_unique<UncompressedTableReader>(
      options.env->MakeReadOnlyIO(writer.GetFileName()), writer.GetIndex());
}

}  // namespace mdb
