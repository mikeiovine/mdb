#pragma once

namespace mdb {
namespace constants {

// These are used by the logfile/sstable writers to format
// the table.
inline constexpr char KV_SEP{ '\x01' };
inline constexpr char PAIR_SEP{ '\x02' };
inline constexpr char DEL_MARKER{ '\x03' };

// If true, fsync() after all calls to write()
inline constexpr bool SYNC_WRITES{ false };

} // namespace constants
} // namespace mdb
