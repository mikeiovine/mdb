#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "file.h"
#include "helpers.h"
#include "types.h"

namespace mdb {

class TableWriter {
 public:
  TableWriter() = default;

  TableWriter(const TableWriter&) = delete;
  TableWriter& operator=(const TableWriter&) = delete;

  TableWriter(TableWriter&&) = delete;
  TableWriter& operator=(TableWriter&&) = delete;

  virtual ~TableWriter() = default;

  virtual void WriteMemtable(const MemTableT& memtable) = 0;

  virtual IndexT GetIndex() const = 0;

  virtual std::string GetFileName() const = 0;

  // It is the responsibility of the caller to make sure
  // keys are added in SORTED order! The reader is depending
  // on this invariant. This function will throw std::invalid_argument
  // if this condition is violated
  virtual void Add(std::string_view key, std::string_view value) = 0;

  // Note: if you're adding keys manually via Add(), you'll want to call
  // Flush() when you're done to write the last block to disk.
  virtual void Flush() = 0;

  virtual size_t NumKeys() const noexcept = 0;
};

class UncompressedTableWriter : public TableWriter {
 public:
  UncompressedTableWriter(std::unique_ptr<WriteOnlyIO> file, bool sync,
                          size_t block_size)
      : file_{std::move(file)}, sync_{sync}, block_size_{block_size} {
    assert(file_ != nullptr);
  }

  void WriteMemtable(const MemTableT& memtable) override;

  IndexT GetIndex() const override;

  std::string GetFileName() const override;

  void Add(std::string_view key, std::string_view value) override;

  void Flush() override;

  size_t NumKeys() const noexcept override;

 private:
  std::vector<char> buf_;

  std::unique_ptr<WriteOnlyIO> file_;
  IndexT index_;

  const bool sync_;
  const size_t block_size_;

  size_t cur_index_{0};
  size_t num_keys_{0};
  bool block_marked_{false};

  std::string last_key = "";
};

}  // namespace mdb
