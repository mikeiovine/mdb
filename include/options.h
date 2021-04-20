#pragma once

#include "env.h"
#include "table_factory.h"

#include <memory>
#include <string>

namespace mdb {

struct Options {
    std::shared_ptr<Env> env{ Env::CreateDefault() };

    // If true, force sync with disk (e.g. via a call to fsync())
    // after every write. 
    bool write_sync{ false };

    // Approx. size for sorted tables on disk.
    size_t block_size{ 4096 };

    // Where to write DB files.
    std::string path{ "./" };

    // Max size for logfiles in bytes
    size_t log_max_size{ 4096 * 100 * 100 };

    // Max size for in-memory sorted table
    size_t memtable_max_size{ 4096 * 1000 };

    // Make table readers/writers for the DB.
    std::shared_ptr<TableFactory> table_factory{ std::make_shared<UncompressedTableFactory>() };
};

}
