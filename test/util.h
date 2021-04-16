#pragma once

#include "file.h"
#include <vector>

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

    private:
        bool closed_{ false };
        std::vector<char> input_;
        size_t read_pos_{ 0 };
};
