#include "memtable.h"

#include <cassert>

namespace mdb {

void MemTable::Add(const std::string& key, const std::string& value) {
    assert(key.size() > 0 && value.size() > 0);
    
    if (impl_.find(key) == impl_.end()) {
       size_ += value.size(); 
    }

    impl_[key] = value;
    size_ += key.size();
}

std::string MemTable::ValueOf(const std::string& key) {
    assert(key.size() > 0);

    auto res{ impl_.find(key) };
    if (res == impl_.end()) {
        return "";
    }

    return res->second;
}

size_t MemTable::Size() const noexcept {
    return size_;
}

};
