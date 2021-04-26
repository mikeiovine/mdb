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

  virtual std::unique_ptr<TableReader> TableFromMemtable(
      int table_number, const Options& options, const MemTableT& memtable,
      bool write_deleted) = 0;

  virtual std::unique_ptr<TableWriter> MakeEmptyTableWriter(
      int table_number, const Options& options) = 0;
};

class UncompressedTableFactory : public TableFactory {
 public:
  std::unique_ptr<TableReader> TableFromMemtable(int table_number,
                                                 const Options& options,
                                                 const MemTableT& memtable,
                                                 bool write_deleted) override;

  std::unique_ptr<TableWriter> MakeEmptyTableWriter(
      int table_number, const Options& options) override;
};

}  // namespace mdb
