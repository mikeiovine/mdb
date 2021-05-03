#include "types.h"

namespace mdb {

std::optional<std::string> LookupInMemTable(std::string_view key,
                                            const MemTableT& memtable) {
  auto loc{memtable.find(key)};
  if (loc != memtable.end()) {
    return loc->second;
  }

  return std::nullopt;
}

}  // namespace mdb
