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
  // and returns a reader to the new file. The level passed
  // to the ctor of the new table reader is always 0.
  virtual std::unique_ptr<TableReader> TableFromMemtable(
      size_t table_number, const Options& options,
      const MemTableT& memtable) = 0;

  // Makes an empty table. The level must be specified.
  virtual std::unique_ptr<TableWriter> MakeTableWriter(size_t table_number,
                                                       const Options& options,
                                                       size_t level) = 0;

  // Make a table reader from a writer (use the writer's index and filename)
  virtual std::unique_ptr<TableReader> TableReaderFromWriter(
      const TableWriter& writer, const Options& options) = 0;

  // Make a new table reader. The index is constructed from the file on disk.
  // Error if the file does not exist.
  virtual std::unique_ptr<TableReader> MakeTableReader(
      size_t table_number, const Options& options) = 0;
};

class UncompressedTableFactory : public TableFactory {
 public:
  std::unique_ptr<TableReader> TableFromMemtable(
      size_t table_number, const Options& options,
      const MemTableT& memtable) override;

  std::unique_ptr<TableWriter> MakeTableWriter(size_t table_number,
                                               const Options& options,
                                               size_t level) override;

  std::unique_ptr<TableReader> TableReaderFromWriter(
      const TableWriter& writer, const Options& options) override;

  std::unique_ptr<TableReader> MakeTableReader(size_t table_number,
                                               const Options& options) override;
};

}  // namespace mdb
