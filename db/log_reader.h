#pragma once

#include <memory>
#include <optional>
#include <string>

#include "file.h"
#include "options.h"
#include "types.h"

namespace mdb {

class LogReader {
 public:
  LogReader(size_t log_number, const Options& options);
  explicit LogReader(std::unique_ptr<ReadOnlyIO>&& file);

  MemTableT ReadMemTable();

  std::string GetFileName() const noexcept;

 private:
  std::optional<std::string> ReadNextString();
  std::unique_ptr<ReadOnlyIO> file_;

  size_t pos_{0};
};

}  // namespace mdb
