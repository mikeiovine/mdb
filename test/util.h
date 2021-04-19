#pragma once

#include "file.h"
#include <vector>

inline size_t ReadSizeT(const std::vector<char>& data, size_t offset) {
    const char * buf = data.data() + offset;
    return *reinterpret_cast<const size_t*>(buf);
}

inline std::string ReadString(const std::vector<char>& data, size_t offset, size_t num_bytes) {
    const char * buf = data.data() + offset;
    return std::string(buf, buf + num_bytes);
}

inline void WriteSizeT(std::vector<char>& buf, size_t size) {
    char * size_ptr{ reinterpret_cast<char*>(&size) };
    buf.insert(
        buf.end(),
        size_ptr,
        size_ptr + sizeof(size_t));
}

inline void WriteString(std::vector<char>& buf, const std::string& str) {
    buf.insert(
        buf.end(),
        str.cbegin(),
        str.cend());
}

class WriteOnlyIOMock : public mdb::WriteOnlyIO {
    public:
        WriteOnlyIOMock(std::vector<char>& output) : record_{ output } {}

        void Write(const char * data, size_t size) override {
            if (!closed_) {
                record_.insert(
                    record_.end(),
                    data,
                    data + size
                );
            }
        }

        void Sync() override {
            if (!closed_) {
                for (const auto& func : on_sync_) {
                    func();
                }
            }
        }

        void Close() override {
            closed_ = true;
        }

        void SetOnSync(std::function<void(void)> func) {
            on_sync_.push_back(std::move(func));
        }

    private:
        bool closed_{ false };
        std::vector<char>& record_;
        std::vector<std::function<void(void)>> on_sync_;
};


class ReadOnlyIOMock : public mdb::ReadOnlyIO {
    public:
        ReadOnlyIOMock(std::vector<char> input) : input_{ std::move(input) } {}
        
        size_t Read(char * output, size_t size) override {
            if (!closed_) {
                size_t read_size = 
                    std::min(input_.size() - read_pos_, size);

                std::copy(
                    input_.begin() + read_pos_,
                    input_.begin() + read_pos_ + read_size,
                    output);

                read_pos_ += size;
                return read_size;
            }

            return 0;
        }

        void Close() override {
            closed_ = true;
        }

        void Seek(size_t offset) override {
            read_pos_ = offset;  
        }

    private:
        bool closed_{ false };
        std::vector<char> input_;
        size_t read_pos_{ 0 };
};
