#pragma once

#include "file.h"

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
        // TODO make sure the client does not pass this byte into key/value
        static constexpr char SEPARATOR = '\x01';

        void write_with_sep(const std::string& s);
        WritableFile output_;
};

} // namespace mdb
