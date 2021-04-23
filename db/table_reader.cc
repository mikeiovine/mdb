#include "table_reader.h"

#include <vector>
#include <string>

namespace mdb {

class UncompressedTableReader::UncompressedTableIter : public TableIteratorImpl {
    public:
        UncompressedTableIter(
            UncompressedTableReader& reader,
            IndexT::const_iterator it) :
            reader_{ reader },
            it_{ it } {
            
            if (!IsDone()) {
                JumpToBlock(it_->second);
                SetCur();

            // index_.end() passed in
            } else {
                cur_ = {"", ""};
                pos_ = reader_.file_->Size();
            }
        }

        ValueType& GetValue() override {
            return cur_;
        }

        bool IsDone() override {
            return it_ == reader_.index_.end();
        }

        void Next() override {
            pos_ += cur_size_;
            cur_block_pos_ += cur_size_;

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

        size_t Position() const noexcept override {
            return pos_;
        }

        int GetFileID() const noexcept {
            return reader_.file_->GetID();
        }

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
            size_t key_size{ reader_.ReadSize(pos_) };
            assert(key_size < cur_block_size_);

            cur_.first = reader_.ReadString(key_size, pos_ + sizeof(size_t)); 

            size_t value_size{ reader_.ReadSize(pos_ + key_size + sizeof(size_t)) };

            assert(value_size < cur_block_size_);
            cur_.second = reader_.ReadString(value_size, pos_ + key_size + 2 * sizeof(size_t));

            cur_size_ = key_size + value_size + 2 * sizeof(size_t);
        }

        UncompressedTableReader& reader_;
        IndexT::const_iterator it_;

        ValueType cur_;
        size_t cur_size_;

        size_t cur_block_size_{ 0 };
        size_t cur_block_pos_{ 0 };

        size_t pos_{ 0 }; 
};

std::string UncompressedTableReader::ValueOf(std::string_view key) {
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
    std::string_view key_to_find) {

    // TODO This can probably be re-written in terms of the new iterator
    assert(file_ != nullptr);

    size_t block_size;
    file_->Read(reinterpret_cast<char*>(&block_size), sizeof(size_t), block_loc);

    size_t pos{ sizeof(size_t) };

    while (pos < block_size) {
        size_t key_size{ ReadSize(block_loc + pos) };
        pos += sizeof(size_t); 

        assert(key_size < block_size);

        std::string key{ ReadString(key_size, block_loc + pos) };
        pos += key_size;
        
        size_t value_size{ ReadSize(block_loc + pos) };
        pos += sizeof(size_t);

        assert(value_size < block_size);

        if (key == key_to_find) {
            return ReadString(value_size, block_loc + pos);
        } else {
            pos += value_size;
        }
    }
    
    assert(pos == block_size);
    return "";
}

size_t UncompressedTableReader::ReadSize(size_t offset) {
    size_t size;
    file_->Read(reinterpret_cast<char*>(&size), sizeof(size_t), offset);
    return size;
}

std::string UncompressedTableReader::ReadString(size_t size, size_t offset) {
    std::vector<char> buf;
    buf.reserve(size);

    file_->Read(buf.data(), size, offset);
    return std::string{ buf.data(), size };
}

TableIterator UncompressedTableReader::Begin() {
    return TableIterator(std::make_shared<UncompressedTableIter>(*this, index_.cbegin()));
}

TableIterator UncompressedTableReader::End() {
    return TableIterator(std::make_shared<UncompressedTableIter>(*this, index_.cend()));
}

} // namespace mdb
