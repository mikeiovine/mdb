#include <gtest/gtest.h>

#include <string>

#include "util.h"
#include "log_writer.h"

using namespace mdb;

size_t ReadSizeT(const std::vector<char>& data, size_t offset) {
    const char * buf = data.data() + offset;
    return *reinterpret_cast<const size_t*>(buf);
}

std::string ReadString(const std::vector<char>& data, size_t offset, size_t num_bytes) {
    const char * buf = data.data() + offset;
    return std::string(buf, buf + num_bytes);
}

void CompareKvToOutput(
    const std::vector<char>& write_dest, 
    const std::vector<std::pair<std::string, std::string>>& pairs) {

    size_t cur{ 0 };
    for (const auto& kv : pairs) {
        ASSERT_TRUE(write_dest.size() - cur >= sizeof(size_t));
        
        size_t key_size{ ReadSizeT(write_dest, cur) };
        cur += sizeof(size_t);

        ASSERT_EQ(key_size, kv.first.size());

        ASSERT_TRUE(write_dest.size() - cur >= key_size);
        std::string key{ ReadString(write_dest, cur, key_size) };
        cur += key.size();

        ASSERT_EQ(key, kv.first);

        ASSERT_TRUE(write_dest.size() - cur >= sizeof(size_t));
        size_t value_size{ ReadSizeT(write_dest, cur) };
        cur += sizeof(size_t);

        ASSERT_EQ(value_size, kv.second.size());

        ASSERT_TRUE(write_dest.size() - cur >= value_size);
        std::string value{ ReadString(write_dest, cur, value_size) };
        cur += value.size();

        ASSERT_EQ(value, kv.second);
    }

    ASSERT_EQ(cur, write_dest.size());
}

/**
 * Test that the logfile format is correct with no deletes
 */
TEST(TestLogWriter, TestLogfileFormatNoDeletes) {
    std::vector<char> buf;

    auto io{ std::make_unique<WriteOnlyIOMock>(buf) };
    auto log{ LogWriter(std::move(io), false) };

    std::vector<std::pair<std::string, std::string>> pairs{
        {"some_key", "some_value"},
        {"another_key", "hello_world"},
        {"yet_another_key", "value"}
    };

    for (const auto& kv : pairs) {
        log.Add(kv.first, kv.second);
    }

    log.FlushBuffer();
    CompareKvToOutput(buf, pairs);
}
