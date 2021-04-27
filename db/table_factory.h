#pragma once

#include <memory>

#include "env.h"
#include "types.h"

namespace mdb {

struct Options;
class TableReader;
class TableWriter;

class TableFactory {
 public:
  TableFactory() = default;

  TableFactory(const TableFactory&) = delete;
  TableFactory& operator=(const TableFactory&) = delete;

  TableFactory(TableFactory&&) = delete;
  TableFactory& operator=(TableFactory&&) = delete;

  virtual ~TableFactory() = default;

  // A convenience function that writes an entire memtable
  // and returns a reader to the new file.
  virtual std::unique_ptr<TableReader> TableFromMemtable(
      int table_number, const Options& options, const MemTableT& memtable) = 0;

  // Makes an empty table.
  virtual std::unique_ptr<TableWriter> MakeTableWriter(
      int table_number, const Options& options) = 0;

  // Readers must be constructed from a writer. This is because the writer
  // will construct the block index that the reader will eventually need.
  virtual std::unique_ptr<TableReader> MakeTableReader(
      const TableWriter& writer, const Options& options) = 0;
};

class UncompressedTableFactory : public TableFactory {
 public:
  std::unique_ptr<TableReader> TableFromMemtable(
      int table_number, const Options& options,
      const MemTableT& memtable) override;

  std::unique_ptr<TableWriter> MakeTableWriter(int table_number,
                                               const Options& options) override;

  std::unique_ptr<TableReader> MakeTableReader(const TableWriter& writer,
                                               const Options& options) override;
};

}  // namespace mdb
