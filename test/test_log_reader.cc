#include <gtest/gtest.h>

#include "util.h"
#include "log_reader.h"

#include <vector>
#include <map>

using namespace mdb;

namespace {

using SequenceT = std::vector<std::pair<std::string, std::string>>;
using MemTableT = std::map<std::string, std::string>;

void WriteSize(std::vector<char>& buf, size_t size) {
    char * size_ptr{ reinterpret_cast<char*>(&size) };
    buf.insert(
        buf.end(),
        size_ptr,
        size_ptr + sizeof(size_t));
}

void WriteString(std::vector<char>& buf, const std::string& str) {
    buf.insert(
        buf.end(),
        str.cbegin(),
        str.cend());
}

std::vector<char> ConstructInput(const SequenceT& seq) {
    std::vector<char> buf;

    for (auto& kv : seq) {
        WriteSize(buf, kv.first.size());
        WriteString(buf, kv.first);

        WriteSize(buf, kv.second.size());
        if (kv.second.size() > 0) {
            WriteString(buf, kv.second);
        } 
    }

    return buf;
}

} // namespace

/**
 * Test reading a log with no deletes
 */
TEST(TestLogReader, TestLogReaderNoDeletes) {
    SequenceT input_seq{
        {"abc", "def"},
        {"xyz", "nop"},
        {"hello", "world"},
        {"abc", "overwritten_key"}
    };

    std::vector<char> input{ ConstructInput(input_seq) };
    auto io{ std::make_unique<ReadOnlyIOMock>(input) };
    
    LogReader reader{ std::move(io) };

    MemTableT memtable{ reader.ReadMemTable<MemTableT>() };

    MemTableT expected{
        {"abc", "overwritten_key"},
        {"xyz", "nop"},
        {"hello", "world"}
    };

    ASSERT_EQ(memtable, expected);
}

/**
 * Test reading a log with a few deletes
 */
TEST(TestLogReader, TestLogReaderWithDeletes) {
    SequenceT input_seq{
        {"abc", "def"},
        {"xyz", "nop"},
        {"hello", "world"},
        {"hello", ""},
        {"xyz", ""}
    };

    std::vector<char> input{ ConstructInput(input_seq) };
    auto io{ std::make_unique<ReadOnlyIOMock>(input) };
    
    LogReader reader{ std::move(io) };

    MemTableT memtable{ reader.ReadMemTable<MemTableT>() };

    MemTableT expected{
        {"abc", "def"}
    };

    ASSERT_EQ(memtable, expected);
}

// TODO how to handle corruption?
