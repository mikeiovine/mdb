#include <boost/test/unit_test.hpp>

#include "log_reader.h"
#include "log_writer.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestLogIntegration)

/**
 * Test that the log reader can read the log writer's output.
 */
BOOST_AUTO_TEST_CASE(TestLogReaderReadsLogWriterOutput) {
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};

  LogWriter writer{std::move(io), false};

  std::vector<std::pair<std::string, std::string>> kv{
      {"a", "b"}, {"c", "d"}, {"key", "value"}};

  for (const auto &pair : kv) {
    writer.Add(pair.first, pair.second);
  }
  writer.FlushBuffer();

  auto io_read{std::make_unique<ReadOnlyIOMock>(std::move(output))};
  LogReader reader{std::move(io_read)};

  MemTableT memtable{reader.ReadMemTable()};

  for (const auto &pair : kv) {
    auto it{memtable.find(pair.first)};
    BOOST_REQUIRE(it != memtable.end());
    BOOST_REQUIRE_EQUAL(it->second, pair.second);
  }
}

BOOST_AUTO_TEST_SUITE_END()
