#pragma once

#include <optional>
#include <string>

#include "file.h"
#include "iterator.h"
#include "types.h"

namespace mdb {

class TableReader {
 public:
  TableReader() = default;

  TableReader(const TableReader&) = delete;
  TableReader& operator=(const TableReader&) = delete;

  TableReader(TableReader&&) = delete;
  TableReader& operator=(TableReader&&) = delete;

  virtual ~TableReader() = default;

  virtual std::optional<std::string> ValueOf(std::string_view key) = 0;

  virtual TableIterator Begin() = 0;
  virtual TableIterator End() = 0;

  virtual size_t Size() const = 0;

  virtual std::string GetFileName() const noexcept { return ""; }
};

class UncompressedTableReader : public TableReader {
 public:
  UncompressedTableReader(std::unique_ptr<ReadOnlyIO> file, IndexT index)
      : file_{std::move(file)}, index_{std::move(index)} {}

  std::optional<std::string> ValueOf(std::string_view key) override;

  TableIterator Begin() override;
  TableIterator End() override;

  size_t Size() const override;

 private:
  class UncompressedTableIter;

  std::optional<std::string> SearchInBlock(size_t block_loc,
                                           std::string_view key_to_find);

  std::string ReadString(size_t size, size_t offset);
  size_t ReadSize(size_t offset);
  std::string GetFileName() const noexcept override;

  std::unique_ptr<ReadOnlyIO> file_;
  const IndexT index_;
};

}  // namespace mdb
