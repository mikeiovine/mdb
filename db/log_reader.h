#pragma once

#include <memory>
#include <string>
#include <optional>

#include "file.h"
#include "types.h"
#include "options.h"

namespace mdb {

class LogReader {
    public:
        LogReader(int log_number, const Options& options);
        LogReader(std::unique_ptr<ReadOnlyIO> file);
        
        MemTableT ReadMemTable();

        std::string GetFileName() const noexcept;

    private:
        std::optional<std::string> ReadNextString();
        std::unique_ptr<ReadOnlyIO> file_;

        size_t pos_{ 0 };
};

} // namespace mdb
