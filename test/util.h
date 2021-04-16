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
        }

        void Close() override {}

    private:
        std::vector<char>& record_;
};
