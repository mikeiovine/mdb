#include "table_writer.h"

namespace mdb {

IndexT UncompressedTableWriter::GetIndex() const { return index_; }

void UncompressedTableWriter::WriteMemtable(const MemTableT& memtable,
                                            bool write_deleted) {
  for (auto it = memtable.cbegin(); it != memtable.cend(); it++) {
    // Empty values correspond to deleted keys.
    if (write_deleted || !it->second.empty()) {
      Add(it->first, it->second);
    }
  }

  if (buf_.size() > 0) {
    Flush();
  }
}

void UncompressedTableWriter::Add(std::string_view key,
                                  std::string_view value) {
  assert(key.size() > 0);
  num_keys_++;

  // Placeholder bytes; we'll put the real size when we flush
  if (buf_.empty()) {
    size_t size{0};
    buf_.insert(buf_.end(), &size, &size + sizeof(size_t));
  }

  if (!block_marked_) {
    index_.emplace(key, cur_index_);
    block_marked_ = true;
  }

  util::AddStringToWritable(key, buf_);
  util::AddStringToWritable(value, buf_);

  if (buf_.size() >= block_size_) {
    block_marked_ = false;
    cur_index_ += buf_.size();
    Flush();
  }
}

void UncompressedTableWriter::Flush() {
  assert(file_ != nullptr);
  assert(buf_.size() >= sizeof(size_t));

  *reinterpret_cast<size_t*>(buf_.data()) = buf_.size() - sizeof(size_t);
  file_->Write(buf_.data(), buf_.size());
  if (sync_) {
    file_->Sync();
  }

  buf_.clear();
}

std::string UncompressedTableWriter::GetFileName() const {
  return file_->GetFileName();
}

size_t UncompressedTableWriter::NumKeys() const noexcept { return num_keys_; }

}  // namespace mdb
