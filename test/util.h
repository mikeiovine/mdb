#pragma once

#include "file.h"
#include <vector>

class WriteOnlyIOMock : public mdb::WriteOnlyIO {
    public:
        WriteOnlyIOMock(std::vector<char>& output) : record_{ output } {}

        void Write(const char * data, size_t size) override {
            record_.insert(
                record_.end(),
                data,
                data + size
            );
        }

        void Sync() override {
            for (const auto& func : on_sync_) {
                func();
            }
        }

        void Close() override {}

        void SetOnSync(std::function<void(void)> func) {
            on_sync_.push_back(std::move(func));
        }

    private:
        std::vector<char>& record_;
        std::vector<std::function<void(void)>> on_sync_;
};


class ReadOnlyIOMock : public mdb::ReadOnlyIO {
    public:
        ReadOnlyIOMock(std::vector<char> input) : input_{ std::move(input) } {}
        
        bool Read(char * output, size_t size) override {
            if (input_.size() - read_pos_ < size) {
                return false;
            }

            std::copy(
                input_.begin() + read_pos_,
                input_.begin() + read_pos_ + size,
                output);

            read_pos_ += size;
            return true;
        }

        void Close() override {}

    private:
        std::vector<char> input_;
        size_t read_pos_{ 0 };
};
