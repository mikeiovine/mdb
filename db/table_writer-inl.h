#pragma once

namespace mdb {

template <class MemTableT>
void UncompressedTableWriter<MemTableT>::WriteMemtable(const MemTableT& memtable) {
    for (auto it = memtable.cbegin(); it != memtable.cend(); it++) {
        Add(it->first, it->second);
        if (buf_.size() >= block_size_) {
            Flush();
        } 
    }
    Flush();
}

template <class MemTableT>
void UncompressedTableWriter<MemTableT>::Add(const std::string& key, const std::string& value) {
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

template <class MemTableT>
void UncompressedTableWriter<MemTableT>::Flush() {
    assert(file_ != nullptr);
    
    if (buf_.size() > 0) {
        assert(buf_.size() >= sizeof(size_t));
        *reinterpret_cast<size_t*>(buf_.data()) = buf_.size() - sizeof(size_t);
        file_->Write(buf_.data(), buf_.size());
        if (sync_) {
            file_->Sync();
        }
    }

    buf_.clear();
}

} // namespace mdb
