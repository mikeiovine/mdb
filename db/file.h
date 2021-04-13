#pragma once

#include <string>

namespace mdb {

class WritableFile {
    public:
        WritableFile(const std::string& filename);

        WritableFile(const WritableFile&) = delete;
        WritableFile& operator=(const WritableFile&) = delete; 

        WritableFile(WritableFile&&) = delete;
        WritableFile& operator=(WritableFile&&) = delete;

        ~WritableFile();

        void write(const char * data, size_t size);
        void close();

    private:
        const int fd_;
        bool closed{ false };
};

} // namespace mdb
