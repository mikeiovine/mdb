#include <gtest/gtest.h>

#include "log_reader.h"
#include "log_writer.h"
#include "util.h"

using namespace mdb;

/**
 * Test that the log reader can read the log writer's output.
 */
TEST(TestLogIntegration, TestLogReaderReadsLogWriterOutput) {
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};

  LogWriter writer{std::move(io), false};

  std::vector<std::pair<std::string, std::string>> kv{
      {"a", "b"}, {"c", "d"}, {"key", "value"}};

  for (const auto& pair : kv) {
    writer.Add(pair.first, pair.second);
  }
  writer.FlushBuffer();

  auto io_read{std::make_unique<ReadOnlyIOMock>(std::move(output))};
  LogReader reader{std::move(io_read)};

  MemTableT memtable{reader.ReadMemTable()};

  for (const auto& pair : kv) {
    auto it{memtable.find(pair.first)};
    ASSERT_NE(it, memtable.end());
    ASSERT_EQ(it->second, pair.second);
  }
}
