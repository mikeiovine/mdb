#include "table_factory.h"

#include "options.h"
#include "table_reader.h"
#include "table_writer.h"

namespace mdb {

std::unique_ptr<TableReader> UncompressedTableFactory::TableFromMemtable(
    size_t table_number, const Options& options, const MemTableT& memtable) {
  UncompressedTableWriter writer{
      options.env->MakeWriteOnlyIO(util::TableFileName(options, table_number)),
      options.write_sync, options.block_size, 0};

  writer.WriteMemtable(memtable);

  return std::make_unique<UncompressedTableReader>(
      options.env->MakeReadOnlyIO(writer.GetFileName()), writer.GetIndex());
}

std::unique_ptr<TableWriter> UncompressedTableFactory::MakeTableWriter(
    size_t table_number, const Options& options, size_t level) {
  return std::make_unique<UncompressedTableWriter>(
      options.env->MakeWriteOnlyIO(util::TableFileName(options, table_number)),
      options.write_sync, options.block_size, level);
}

std::unique_ptr<TableReader> UncompressedTableFactory::TableReaderFromWriter(
    const TableWriter& writer, const Options& options) {
  return std::make_unique<UncompressedTableReader>(
      options.env->MakeReadOnlyIO(writer.GetFileName()), writer.GetIndex());
}

std::unique_ptr<TableReader> UncompressedTableFactory::MakeTableReader(
    size_t table_number, const Options& options) {
  return std::make_unique<UncompressedTableReader>(
      options.env->MakeReadOnlyIO(util::TableFileName(options, table_number)));
}

}  // namespace mdb
