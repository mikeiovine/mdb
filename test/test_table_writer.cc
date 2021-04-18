#include <gtest/gtest.h>

#include <map>
#include <cmath>
#include <iostream>
#include <string>

#include "table_writer.h"
#include "util.h"

using namespace mdb;

using MemTableT = std::map<std::string, std::string>;
using UncompressedTableWriterT = UncompressedTableWriter<MemTableT>;
using MemTableOrderedT = std::vector<std::pair<std::string, std::string>>;

/**
 * Assert that the number of blocks in the buffer is correct.
 * The buffer is assumed to be in the "uncompressed" format.
 */
void CheckNumBlocksUncompressed(const std::vector<char>& output, int expected) {
    int n{ 0 };
    size_t pos{ 0 };
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
void ReadUncompressedBlock(
    const std::vector<char>& buf, 
    size_t pos, 
    size_t block_size, 
    MemTableOrderedT& output) {

    size_t i{ 0 };
    while (i < block_size) {
        ASSERT_TRUE(buf.size() - (pos + i) >= sizeof(size_t));
        size_t key_size{ ReadSizeT(buf, pos + i) };
        i += sizeof(size_t);

        ASSERT_TRUE(buf.size() - (pos + i) >= key_size);
        std::string key{ ReadString(buf, pos + i, key_size) };
        i += key_size;

        ASSERT_TRUE(buf.size() - (pos + i) >= sizeof(size_t));
        size_t value_size{ ReadSizeT(buf, pos + i) };
        i += sizeof(size_t);

        ASSERT_TRUE(buf.size() - (pos + i) >= value_size);
        std::string value{ ReadString(buf, pos + i, value_size) };
        i += value_size;

        output.push_back({key, value});
    }

    ASSERT_EQ(i, block_size);
}

/**
 * Verify that an uncompressed output is formatted correctly.
 */
void CompareUncompressedOutput(const std::vector<char>& buf, const MemTableOrderedT& expected) {
    size_t pos{ 0 };
    
    MemTableOrderedT res;

    while (pos < buf.size()) {
        ASSERT_TRUE(buf.size() - pos >= sizeof(size_t));

        size_t block_size{ ReadSizeT(buf, pos) };
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
    auto io{ std::make_unique<WriteOnlyIOMock>(output) };
    bool sync{ false };
    size_t block_size{ 16 + 5 * sizeof(size_t) };

    MemTableT to_write{
        // First block
        {"1", "1234567891"},
        {"2", "efgh"},
        // Second
        {"3", std::string('1', block_size + 1)},
        // Third
        {"4", "a"},
        {"5", "b"}
    };

    UncompressedTableWriterT writer{
        std::move(io),
        sync,
        block_size };

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
    auto io{ std::make_unique<WriteOnlyIOMock>(output) };
    bool sync{ false };
    size_t block_size{ 16 + 5 * sizeof(size_t) };

    MemTableT to_write{
        // First block
        {"1", "1234567891"},
        {"2", "efgh"},
        // Second
        {"3", std::string('1', block_size + 1)},
        // Third
        {"4", "a"},
        {"5", "b"}
    };

    UncompressedTableWriterT writer{
        std::move(io),
        sync,
        block_size };

    writer.WriteMemtable(to_write);

    CompareUncompressedOutput(
        output, 
        MemTableOrderedT(to_write.begin(), 
        to_write.end()));
}
