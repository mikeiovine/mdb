#include <boost/test/unit_test.hpp>
#include <limits>
#include <map>
#include <vector>

#include "log_reader.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestLogReader)

namespace {

using SequenceT = std::vector<std::pair<std::string, std::string>>;

size_t PairSize(const std::pair<std::string, std::string> &pair) {
  return pair.first.size() + pair.second.size();
}

std::vector<char> ConstructInput(const SequenceT &seq) {
  std::vector<char> buf;

  for (auto &kv : seq) {
    WriteSizeT(buf, kv.first.size());
    WriteString(buf, kv.first);

    WriteSizeT(buf, kv.second.size());
    if (kv.second.size() > 0) {
      WriteString(buf, kv.second);
    }
  }

  return buf;
}

}  // namespace

/**
 * Test reading a log with no deletes
 */
BOOST_AUTO_TEST_CASE(TestLogReaderNoDeletes) {
  SequenceT input_seq{{"abc", "def"},
                      {"xyz", "nop"},
                      {"hello", "world"},
                      {"abc", "overwritten_key"}};

  std::vector<char> input{ConstructInput(input_seq)};
  auto io{std::make_unique<ReadOnlyIOMock>(input)};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};

  MemTableT expected{
      {"abc", "overwritten_key"}, {"xyz", "nop"}, {"hello", "world"}};

  BOOST_TEST_REQUIRE(expected == memtable, boost::test_tools::per_element());
}

/**
 * Test reading a log with a few deletes
 */
BOOST_AUTO_TEST_CASE(TestLogReaderWithDeletes) {
  SequenceT input_seq{{"abc", "def"},
                      {"xyz", "nop"},
                      {"hello", "world"},
                      {"hello", ""},
                      {"xyz", ""}};

  std::vector<char> input{ConstructInput(input_seq)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};

  MemTableT expected{{"abc", "def"}};

  BOOST_TEST_REQUIRE(expected == memtable, boost::test_tools::per_element());
}

/**
 * The LogReader should gracefully handle corrupted files. It should
 * read valid entries until corruption is encountered.
 */

/**
 * Test reading a logfile with corrupted string sizes (huge number, bigger
 * than the available file space)
 */
BOOST_AUTO_TEST_CASE(TestLogReaderCorruptionHugeKeySize) {
  SequenceT input_seq{{"abc", "def"},
                      {"notdeleted", "val"},
                      {"notdeleted", ""},
                      {"notadded", "val"}};

  std::vector<char> input{ConstructInput(input_seq)};

  // Insert after first 2 keys
  size_t insert_at{4 * sizeof(size_t) + PairSize(input_seq[0]) +
                   PairSize(input_seq[1])};

  size_t corrupted_size{input.size() - insert_at + 1};

  *reinterpret_cast<size_t *>(input.data() + insert_at) = corrupted_size;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};
  MemTableT expected{{"abc", "def"}, {"notdeleted", "val"}};

  BOOST_TEST_REQUIRE(expected == memtable, boost::test_tools::per_element());
}

/**
 * Like TestLogReaderCorruputionHugeValueSize, but puts
 * the problematic size bytes before a key.
 */
BOOST_AUTO_TEST_CASE(TestLogReaderCorruptionHugeValueSize) {
  SequenceT input_seq{{"abc", "def"},
                      {"notdeleted", "val"},
                      {"notdeleted", ""},
                      {"notadded", "val"}};

  std::vector<char> input{ConstructInput(input_seq)};

  // Insert after first 2 keys
  size_t insert_at{5 * sizeof(size_t) + PairSize(input_seq[0]) +
                   PairSize(input_seq[1]) + input_seq[2].first.size()};

  size_t corrupted_size{input.size() - insert_at + 1};

  *reinterpret_cast<size_t *>(input.data() + insert_at) = corrupted_size;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};
  MemTableT expected{{"abc", "def"}, {"notdeleted", "val"}};

  BOOST_TEST_REQUIRE(expected == memtable, boost::test_tools::per_element());
}

/**
 * Test that the log reader fails to read a key without a value
 */
BOOST_AUTO_TEST_CASE(TestLogReaderCorruptionKeyWithNoValue) {
  SequenceT input_seq{{"abc", "def"}, {"ghi", "jkl"}};

  std::vector<char> input{ConstructInput(input_seq)};
  // Erase the last key
  input.erase(input.cend() - input_seq[1].second.size(), input.cend());

  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};
  MemTableT expected{{"abc", "def"}};

  BOOST_TEST_REQUIRE(expected == memtable, boost::test_tools::per_element());
}

BOOST_AUTO_TEST_SUITE_END()
