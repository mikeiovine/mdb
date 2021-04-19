#include "table_reader.h"

#include <vector>
#include <string>

namespace mdb {

std::string UncompressedTableReader::ValueOf(const std::string& key) {
    auto lwr{ index_.upper_bound(key) }; 
    if (lwr != index_.begin()) {
        --lwr;
    } else {
        return "";
    }

    size_t block_loc{ lwr->second };
    return SearchInBlock(block_loc, key);
}

std::string UncompressedTableReader::SearchInBlock(
    size_t block_loc,     
    const std::string& key_to_find) {

    assert(file_ != nullptr);
    file_->Seek(block_loc);

    size_t block_size;
    file_->Read(reinterpret_cast<char*>(&block_size), sizeof(size_t));

    size_t pos{ 0 };

    while (pos < block_size) {
        size_t key_size;
        file_->Read(reinterpret_cast<char*>(&key_size), sizeof(size_t));
        pos += sizeof(size_t); 

        assert(key_size < block_size);

        std::vector<char> key_buf;
        key_buf.reserve(key_size);

        file_->Read(key_buf.data(), key_size);
        std::string key{ key_buf.data(), key_size };

        pos += key_size;
        
        size_t value_size;
        file_->Read(reinterpret_cast<char*>(&value_size), sizeof(size_t));
        pos += sizeof(size_t);

        assert(value_size < block_size);

        if (key == key_to_find) {
            std::vector<char> value_buf;
            value_buf.reserve(value_size);
            file_->Read(value_buf.data(), value_size);
            return std::string{ value_buf.data(), value_size };
        } else {
            pos += value_size;
            file_->Seek(block_loc + pos + sizeof(size_t));
        }
    }
    
    assert(pos == block_size);
    return "";
}

} // namespace mdb
