#include "table_writer.h"

namespace mdb {

std::map<std::string, size_t> UncompressedTableWriter::GetIndex() {
    return index_;
}

void UncompressedTableWriter::WriteMemtable(const MemTableT& memtable) {
    size_t block_marked{ false };

    for (auto it = memtable.cbegin(); it != memtable.cend(); it++) {
        if (!block_marked) {
            index_[it->first] = cur_index_;
            block_marked = true;
        }

        Add(it->first, it->second);

        if (buf_.size() >= block_size_) {
            block_marked = false;
            cur_index_ += buf_.size();
            Flush();
        } 
    }

    if (buf_.size() > 0) {
        Flush();
    }
}

void UncompressedTableWriter::Add(const std::string& key, const std::string& value) {
    assert(key.size() > 0 && value.size() > 0); 
    
    // Placeholder bytes; we'll put the real size when we flush
    if (buf_.empty()) {
        size_t size{ 0 };
        buf_.insert(
            buf_.end(),
            &size,
            &size + sizeof(size_t));    
    }

    util::AddStringToWritable(key, buf_); 
    util::AddStringToWritable(value, buf_); 
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

} // namespace mdb
