#include <gtest/gtest.h>

#include <limits>
#include <map>
#include <vector>

#include "log_reader.h"
#include "util.h"

using namespace mdb;

namespace {

using SequenceT = std::vector<std::pair<std::string, std::string>>;

size_t PairSize(const std::pair<std::string, std::string>& pair) {
  return pair.first.size() + pair.second.size();
}

std::vector<char> ConstructInput(const SequenceT& seq) {
  std::vector<char> buf;

  for (auto& kv : seq) {
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
TEST(TestLogReader, TestLogReaderNoDeletes) {
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

  ASSERT_EQ(memtable, expected);
}

/**
 * Test reading a log with a few deletes
 */
TEST(TestLogReader, TestLogReaderWithDeletes) {
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

  ASSERT_EQ(memtable, expected);
}

/**
 * The LogReader should gracefully handle corrupted files. It should
 * read valid entries until corruption is encountered.
 */

/**
 * Test reading a logfile with corrupted string sizes (huge number, bigger
 * than the available file space)
 */
TEST(TestLogReader, TestLogReaderCorruptionHugeKeySize) {
  SequenceT input_seq{{"abc", "def"},
                      {"notdeleted", "val"},
                      {"notdeleted", ""},
                      {"notadded", "val"}};

  std::vector<char> input{ConstructInput(input_seq)};

  // Insert after first 2 keys
  size_t insert_at{4 * sizeof(size_t) + PairSize(input_seq[0]) +
                   PairSize(input_seq[1])};

  size_t corrupted_size{input.size() - insert_at + 1};

  *reinterpret_cast<size_t*>(input.data() + insert_at) = corrupted_size;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};
  MemTableT expected{{"abc", "def"}, {"notdeleted", "val"}};

  ASSERT_EQ(memtable, expected);
}

/**
 * Like TestLogReaderCorruputionHugeValueSize, but puts
 * the problematic size bytes before a key.
 */
TEST(TestLogReader, TestLogReaderCorruptionHugeValueSize) {
  SequenceT input_seq{{"abc", "def"},
                      {"notdeleted", "val"},
                      {"notdeleted", ""},
                      {"notadded", "val"}};

  std::vector<char> input{ConstructInput(input_seq)};

  // Insert after first 2 keys
  size_t insert_at{5 * sizeof(size_t) + PairSize(input_seq[0]) +
                   PairSize(input_seq[1]) + input_seq[2].first.size()};

  size_t corrupted_size{input.size() - insert_at + 1};

  *reinterpret_cast<size_t*>(input.data() + insert_at) = corrupted_size;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};
  MemTableT expected{{"abc", "def"}, {"notdeleted", "val"}};

  ASSERT_EQ(memtable, expected);
}

/**
 * Test that the log reader fails to read a key without a value
 */
TEST(TestLogReader, TestLogReaderCorruptionKeyWithNoValue) {
  SequenceT input_seq{{"abc", "def"}, {"ghi", "jkl"}};

  std::vector<char> input{ConstructInput(input_seq)};
  // Erase the last key
  input.erase(input.cend() - input_seq[1].second.size(), input.cend());

  auto io{std::make_unique<ReadOnlyIOMock>(std::move(input))};

  LogReader reader{std::move(io)};

  MemTableT memtable{reader.ReadMemTable()};
  MemTableT expected{{"abc", "def"}};

  ASSERT_EQ(memtable, expected);
}
