#include <boost/test/unit_test.hpp>
#include <string>

#include "log_writer.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestLogWriter)

void CompareKvToOutput(
    const std::vector<char>& write_dest,
    const std::vector<std::pair<std::string, std::string>>& pairs) {
  size_t cur{0};
  for (const auto& kv : pairs) {
    BOOST_REQUIRE(write_dest.size() - cur >= sizeof(size_t));

    size_t key_size{ReadSizeT(write_dest, cur)};
    cur += sizeof(size_t);

    BOOST_REQUIRE_EQUAL(key_size, kv.first.size());

    BOOST_REQUIRE(write_dest.size() - cur >= key_size);
    std::string key{ReadString(write_dest, cur, key_size)};
    cur += key.size();

    BOOST_REQUIRE_EQUAL(key, kv.first);

    BOOST_REQUIRE(write_dest.size() - cur >= sizeof(size_t));
    size_t value_size{ReadSizeT(write_dest, cur)};
    cur += sizeof(size_t);

    BOOST_REQUIRE_EQUAL(value_size, kv.second.size());

    BOOST_REQUIRE(write_dest.size() - cur >= value_size);
    std::string value{ReadString(write_dest, cur, value_size)};
    cur += value.size();

    BOOST_REQUIRE_EQUAL(value, kv.second);
  }

  BOOST_REQUIRE_EQUAL(cur, write_dest.size());
}

/**
 * Test that the logfile format is correct with no deletes
 */
BOOST_AUTO_TEST_CASE(TestLogfileFormatNoDeletes) {
  std::vector<char> buf;

  auto io{std::make_unique<WriteOnlyIOMock>(buf)};
  auto log{LogWriter(std::move(io), false)};

  std::vector<std::pair<std::string, std::string>> pairs{
      {"some_key", "some_value"},
      {"another_key", "hello_world"},
      {"yet_another_key", "value"}};

  for (const auto& kv : pairs) {
    log.Add(kv.first, kv.second);
  }

  log.FlushBuffer();
  CompareKvToOutput(buf, pairs);
}

/**
 * Test that the logfile format is correct when deletes are included
 */
BOOST_AUTO_TEST_CASE(TestLogfileFormatWithDeletes) {
  std::vector<char> buf;

  auto io{std::make_unique<WriteOnlyIOMock>(buf)};
  auto log{LogWriter(std::move(io), false)};

  std::vector<std::pair<std::string, std::string>> pairs{
      {"some_key", "some_value"},
      {"another_key", "hello_world"},
      {"yet_another_key", "value"},
      {"some_key", ""},
      {"hello_world", ""},
      {"abc", "someveryveryverylongvalueabcdefgjhi123456789"}};

  for (const auto& kv : pairs) {
    log.Add(kv.first, kv.second);
  }

  log.FlushBuffer();
  CompareKvToOutput(buf, pairs);
}

/**
 * Test adding lots of small records.
 * Effectively tests the automatic flushing mechanism.
 */
BOOST_AUTO_TEST_CASE(TestLogfileFormatManySmallRecords) {
  std::vector<char> buf;

  auto io{std::make_unique<WriteOnlyIOMock>(buf)};
  auto log{LogWriter(std::move(io), false)};

  std::vector<std::pair<std::string, std::string>> pairs(10000, {"ab", "c"});

  for (const auto& kv : pairs) {
    log.Add(kv.first, kv.second);
  }

  log.FlushBuffer();
  CompareKvToOutput(buf, pairs);
}

/**
 * Test that automatic syncing happens for all records when
 * the user passes sync == true
 */
BOOST_AUTO_TEST_CASE(TestLogfileAutoSync) {
  std::vector<char> buf;

  int num_syncs{0};
  auto on_sync{[&num_syncs] { num_syncs += 1; }};

  auto io{std::make_unique<WriteOnlyIOMock>(buf)};
  io->SetOnSync(std::move(on_sync));

  auto log{LogWriter(std::move(io), true)};

  std::vector<std::pair<std::string, std::string>> pairs{
      {"abcdefg", ""},
      {"qwerty", "some_value"},
      {"helloworld", "another_value"}};

  int i{1};
  for (const auto& kv : pairs) {
    log.Add(kv.first, kv.second);
    BOOST_REQUIRE_EQUAL(num_syncs, i);
    i += 1;
  }
}

BOOST_AUTO_TEST_SUITE_END()
