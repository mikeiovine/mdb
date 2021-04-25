#include "table_reader.h"

#include <string>
#include <vector>

namespace mdb {

namespace {

inline void ThrowIOError() {
  // 5 == IO error.
  throw std::system_error(5, std::generic_category());
}

}  // namespace

class UncompressedTableReader::UncompressedTableIter
    : public TableIteratorImpl {
 public:
  UncompressedTableIter(UncompressedTableReader& reader,
                        IndexT::const_iterator it)
      : reader_{reader}, it_{it} {
    if (it != reader_.index_.end()) {
      JumpToBlock(it_->second);
      SetCur();

      // index_.end() passed in
    } else {
      cur_ = {"", ""};
      pos_ = reader_.file_->Size();
    }
  }

  ValueType& GetValue() override { return cur_; }

  bool IsDone() override { return it_ == reader_.index_.end(); }

  void Next() override {
    pos_ += cur_size_;
    cur_block_pos_ += cur_size_;

    assert(cur_block_pos_ <= cur_block_size_);

    if (cur_block_pos_ == cur_block_size_) {
      it_++;
      if (!IsDone()) {
        JumpToBlock(it_->second);
      }
    }

    if (!IsDone()) {
      SetCur();
    }
  }

  size_t Position() const noexcept override { return pos_; }

  int GetFileID() const noexcept { return reader_.file_->GetID(); }

  bool operator==(const TableIteratorImpl& other) override {
    if (typeid(*this) == typeid(other)) {
      const UncompressedTableIter& other_cast =
          static_cast<const UncompressedTableIter&>(other);
      return pos_ == other_cast.pos_ && GetFileID() == other_cast.GetFileID();
    }
    return false;
  }

  std::shared_ptr<TableIteratorImpl> Clone() override {
    return std::make_shared<UncompressedTableIter>(reader_, it_);
  }

 private:
  void JumpToBlock(size_t block_loc) {
    cur_block_pos_ = 0;
    cur_block_size_ = reader_.ReadSize(block_loc);
    pos_ = block_loc + sizeof(size_t);
  }

  void SetCur() {
    size_t key_size{reader_.ReadSize(pos_)};
    assert(key_size < cur_block_size_);

    cur_.first = reader_.ReadString(key_size, pos_ + sizeof(size_t));

    size_t value_size{reader_.ReadSize(pos_ + key_size + sizeof(size_t))};

    assert(value_size < cur_block_size_);
    cur_.second =
        reader_.ReadString(value_size, pos_ + key_size + 2 * sizeof(size_t));

    cur_size_ = key_size + value_size + 2 * sizeof(size_t);
  }

  UncompressedTableReader& reader_;
  IndexT::const_iterator it_;

  ValueType cur_;
  size_t cur_size_;

  size_t cur_block_size_{0};
  size_t cur_block_pos_{0};

  size_t pos_{0};
};

std::optional<std::string> UncompressedTableReader::ValueOf(
    std::string_view key) {
  auto lwr{index_.upper_bound(key)};
  if (lwr != index_.begin()) {
    --lwr;
  } else {
    return std::nullopt;
  }

  size_t block_loc{lwr->second};
  return SearchInBlock(block_loc, key);
}

std::optional<std::string> UncompressedTableReader::SearchInBlock(
    size_t block_loc, std::string_view key_to_find) {
  assert(file_ != nullptr);

  size_t block_size;
  file_->Read(reinterpret_cast<char*>(&block_size), sizeof(size_t), block_loc);

  if (block_size > file_->Size() - sizeof(size_t)) {
    ThrowIOError();
  }

  size_t pos{0};
  block_loc += sizeof(size_t);

  while (pos < block_size) {
    size_t key_size{ReadSize(block_loc + pos)};
    pos += sizeof(size_t);

    std::string key{ReadString(key_size, block_loc + pos)};
    pos += key_size;

    size_t value_size{ReadSize(block_loc + pos)};
    pos += sizeof(size_t);

    if (key == key_to_find) {
      return ReadString(value_size, block_loc + pos);
    } else {
      pos += value_size;
    }
  }

  if (pos > block_size) {
    ThrowIOError();
  }

  return std::nullopt;
}

size_t UncompressedTableReader::ReadSize(size_t offset) {
  size_t size;
  size_t bytes_read{
      file_->Read(reinterpret_cast<char*>(&size), sizeof(size_t), offset)};
  if (bytes_read != sizeof(size_t)) {
    ThrowIOError();
  }
  return size;
}

std::string UncompressedTableReader::ReadString(size_t size, size_t offset) {
  std::vector<char> buf;
  buf.reserve(size);

  size_t bytes_read{file_->Read(buf.data(), size, offset)};
  if (bytes_read != size) {
    ThrowIOError();
  }
  return std::string{buf.data(), size};
}

TableIterator UncompressedTableReader::Begin() {
  return TableIterator(
      std::make_shared<UncompressedTableIter>(*this, index_.cbegin()));
}

TableIterator UncompressedTableReader::End() {
  return TableIterator(
      std::make_shared<UncompressedTableIter>(*this, index_.cend()));
}

size_t UncompressedTableReader::Size() const { return file_->Size(); }

std::string UncompressedTableReader::GetFileName() const noexcept {
  return file_->GetFileName();
}

}  // namespace mdb
