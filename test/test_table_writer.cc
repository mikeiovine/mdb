#include <gtest/gtest.h>

#include <cmath>
#include <iostream>
#include <map>
#include <string>

#include "table_writer.h"
#include "util.h"

using namespace mdb;

using MemTableOrderedT = std::vector<std::pair<std::string, std::string>>;

/**
 * Assert that the number of blocks in the buffer is correct.
 * The buffer is assumed to be in the "uncompressed" format.
 */
void CheckNumBlocksUncompressed(const std::vector<char>& output, int expected) {
  int n{0};
  size_t pos{0};
  while (pos < output.size()) {
    ASSERT_TRUE(output.size() - pos >= sizeof(size_t));
    pos += ReadSizeT(output, pos) + sizeof(size_t);
    n += 1;
  }
  ASSERT_EQ(n, expected);
}

/**
 * Read the uncompressed block starting at "pos"
 */
void ReadUncompressedBlock(const std::vector<char>& buf, size_t pos,
                           size_t block_size, MemTableOrderedT& output) {
  size_t i{0};
  while (i < block_size) {
    ASSERT_TRUE(buf.size() - (pos + i) >= sizeof(size_t));
    size_t key_size{ReadSizeT(buf, pos + i)};
    i += sizeof(size_t);

    ASSERT_TRUE(buf.size() - (pos + i) >= key_size);
    std::string key{ReadString(buf, pos + i, key_size)};
    i += key_size;

    ASSERT_TRUE(buf.size() - (pos + i) >= sizeof(size_t));
    size_t value_size{ReadSizeT(buf, pos + i)};
    i += sizeof(size_t);

    ASSERT_TRUE(buf.size() - (pos + i) >= value_size);
    std::string value{ReadString(buf, pos + i, value_size)};
    i += value_size;

    output.push_back({key, value});
  }

  ASSERT_EQ(i, block_size);
}

/**
 * Verify that an uncompressed output is formatted correctly.
 */
void CompareUncompressedOutput(const std::vector<char>& buf,
                               const MemTableOrderedT& expected) {
  size_t pos{0};

  MemTableOrderedT res;

  while (pos < buf.size()) {
    ASSERT_TRUE(buf.size() - pos >= sizeof(size_t));

    size_t block_size{ReadSizeT(buf, pos)};
    pos += sizeof(size_t);

    ASSERT_TRUE(buf.size() - pos >= block_size);
    ReadUncompressedBlock(buf, pos, block_size, res);
    pos += block_size;
  }

  ASSERT_EQ(res, expected);
}

/**
 * Test that the table writes the correct number of blocks
 */
TEST(TestUncompressedTableWriter, TestCorrectNumBlocks) {
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};
  bool sync{false};
  size_t block_size{16 + 5 * sizeof(size_t)};

  MemTableT to_write{// First block
                     {"1", "1234567891"},
                     {"2", "efgh"},
                     // Second
                     {"3", std::string('1', block_size + 1)},
                     // Third
                     {"4", "a"},
                     {"5", "b"}};

  UncompressedTableWriter writer{std::move(io), sync, block_size};

  writer.WriteMemtable(to_write);

  CheckNumBlocksUncompressed(output, 3);
}

/**
 * Test that the table writes the correct contents:
 * - All keys should be present
 * - The keys should be in sorted order
 */
TEST(TestUncompressedTableWriter, TestContentsBlocks) {
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};
  bool sync{false};
  size_t block_size{16 + 5 * sizeof(size_t)};

  MemTableT to_write{// First block
                     {"1", "1234567891"},
                     {"2", "efgh"},
                     // Second
                     {"3", std::string('1', block_size + 1)},
                     // Third
                     {"4", "a"},
                     {"5", "b"}};

  UncompressedTableWriter writer{std::move(io), sync, block_size};

  writer.WriteMemtable(to_write);

  CompareUncompressedOutput(output,
                            MemTableOrderedT(to_write.begin(), to_write.end()));
}

/**
 * Test the correctness of the TableWriter's index.
 * The index is a map returned by GetIndex. For the uncompressed format,
 * it should store (key, offset) pairs, where offset is the start
 * of the block starting with key.
 */
TEST(TestUncompressedTableWriter, TestIndex) {
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};
  bool sync{false};
  size_t block_size{16 + 5 * sizeof(size_t)};

  MemTableT to_write{// First block
                     {"1", "1234567891"},
                     {"2", "efgh"},
                     // Second
                     {"3", std::string('1', block_size + 1)},
                     // Third
                     {"4", "a"},
                     {"5", "b"}};

  UncompressedTableWriter writer{std::move(io), sync, block_size};

  size_t first_block_size =
      2 + to_write["1"].size() + to_write["2"].size() + 4 * sizeof(size_t);

  size_t second_block_size = 1 + to_write["3"].size() + 2 * sizeof(size_t);

  IndexT expected{
      {"1", 0},
      {"3", first_block_size + sizeof(size_t)},
      {"4", second_block_size + first_block_size + 2 * sizeof(size_t)}};

  writer.WriteMemtable(to_write);
  ASSERT_EQ(expected, writer.GetIndex());
}

/**
 * Test that the NumKeys() method returns the correct number of keys added to
 * the writer.
 */
TEST(TestUncompressedTableWriter, TestNumKeys) {
  bool sync{false};
  size_t block_size{16 + 5 * sizeof(size_t)};

  // Write an entire memtable
  MemTableT to_write{{"1", "v1"}, {"2", "v2"}, {"3", "v3"}};
  std::vector<char> output1;
  auto io1{std::make_unique<WriteOnlyIOMock>(output1)};

  UncompressedTableWriter writer1{std::move(io1), sync, block_size};
  writer1.WriteMemtable(to_write);

  // Write individual key/value pairs
  std::vector<char> output2;
  auto io2{std::make_unique<WriteOnlyIOMock>(output2)};
  UncompressedTableWriter writer2{std::move(io2), sync, block_size};

  writer2.Add("1", "");
  writer2.Add("2", "");

  ASSERT_EQ(writer1.NumKeys(), 3);
  ASSERT_EQ(writer2.NumKeys(), 2);
}

/**
 * Test that the Flush() method really flushes the in-memory buffer and makes
 * a new block when manually adding keys.
 */
TEST(TestUncompressedTableWriter, TestFlush) {
  bool sync{false};
  size_t block_size{16 + 5 * sizeof(size_t)};

  // Write individual key/value pairs
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};
  UncompressedTableWriter writer{std::move(io), sync, block_size};

  writer.Add("1", "");
  writer.Flush();
  writer.Add("2", "");

  ASSERT_EQ(writer.GetIndex().size(), 2);
}
