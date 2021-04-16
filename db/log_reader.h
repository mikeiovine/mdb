#pragma once

#include <memory>
#include <string>
#include <optional>

#include "file.h"

namespace mdb {

class LogReader {
    public:
        LogReader(std::unique_ptr<ReadOnlyIO> file);
        
        template <class MemTableT>
        MemTableT ReadMemTable();

    private:
        std::optional<std::string> ReadNextString();
        std::unique_ptr<ReadOnlyIO> file_;
};

} // namespace mdb

#include "log_reader-inl.h"
