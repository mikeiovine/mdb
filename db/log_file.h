#pragma once

#include "file.h"

#include <memory>

namespace mdb {

class LogFile {
    public:
        LogFile(const std::string& filename);

        LogFile(const LogFile&) = delete;
        LogFile& operator=(const LogFile&) = delete;

        LogFile(LogFile&&) = delete;
        LogFile& operator=(LogFile&&) = delete;

        ~LogFile();

        void add(const std::string& key, const std::string& value);

    private:
        static constexpr char SEPARATOR = '\x01';

        void write_with_sep(const std::string& s);
        void write_with_sep(const char * s, size_t size);
        std::unique_ptr<WritableFile> output_;
};

} // namespace mdb
