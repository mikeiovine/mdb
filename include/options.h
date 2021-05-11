#pragma once

#include <filesystem>
#include <memory>

#include "env.h"
#include "table_factory.h"

namespace mdb {

struct Options {
  std::shared_ptr<Env> env{Env::CreateDefault()};

  // If true, force sync with disk (e.g. via a call to fsync())
  // after every write.
  bool write_sync{false};

  // Approx. size for sorted tables on disk.
  size_t block_size{4096};

  // Where to write DB files.
  std::filesystem::path path{"./db_files"};

  // If true, load the initial database state from the
  // files in path. If this is set to false and path
  // has files in it, an error occurs.
  bool recovery_mode{true};

  // Max size for in-memory sorted table
  size_t memtable_max_size{4096 * 1000};

  // Make table readers/writers for the DB.
  std::shared_ptr<TableFactory> table_factory{
      std::make_shared<UncompressedTableFactory>()};

  // When level 0 has this many tables, a compaction is triggered
  size_t trigger_compaction_at{4};
};

}  // namespace mdb
