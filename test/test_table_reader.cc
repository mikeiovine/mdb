#include <gtest/gtest.h>

#include "util.h"
#include "table_reader.h"

using namespace mdb;

struct BlockT {
    size_t size;
    std::vector<char> data;
};

BlockT ConstructBlock(const std::map<std::string, std::string>& pairs) {
    BlockT block;
    WriteSizeT(block.data, 0);

    for (const auto& kv : pairs) {
        WriteSizeT(block.data, kv.first.size());
        WriteString(block.data, kv.first);

        WriteSizeT(block.data, kv.second.size());
        WriteString(block.data, kv.second);
    }

    block.size = block.data.size() - sizeof(size_t);
    *reinterpret_cast<size_t*>(block.data.data()) = block.size;

    return block;
}

std::vector<char> ConstructTable(const std::vector<BlockT> blocks) {
    std::vector<char> res;

    for (const auto& block : blocks) {
        res.insert(
            res.end(),
            block.data.cbegin(),
            block.data.cend());
    }

    return res;
}

IndexT ConstructIndex(const std::vector<char>& buf) {
    size_t pos{ 0 };
    IndexT idx;

    while (pos < buf.size()) {
        size_t block_size{ ReadSizeT(buf, pos) };
        pos += sizeof(size_t);

        size_t first_key_size{ ReadSizeT(buf, pos) };
        std::string first_key{ ReadString(buf, pos + sizeof(size_t), first_key_size) };
        
        idx[first_key] = pos - sizeof(size_t);
        pos += block_size;
    }

    return idx;
}

TEST(TestUncompressedTableReader, TestTableReaderFindsCorrectKeys) {
    std::vector<std::map<std::string, std::string>> key_values{
        {
            {"abc", "def"},
            {"a", "helloworld"}
        },
        {
            {"b12", "123451251512"},
            {"bbb", "bbbbbbbbbbbbbbbbbbb"}
        },
        {
            {"xyz", "hello"}
        }
    };

    std::vector<BlockT> blocks;
    for (const auto& kv_map : key_values) {
        blocks.push_back(ConstructBlock(kv_map));
    }

    std::vector<char> buf{ ConstructTable(blocks) };

    auto index{ ConstructIndex(buf) };
    auto io{ std::make_unique<ReadOnlyIOMock>(std::move(buf)) }; 

    auto reader{ UncompressedTableReader(std::move(io), std::move(index)) };

    for (const auto& kv_map : key_values) {
        for (const auto& kv : kv_map) {
            const auto& key{ kv.first };
            const auto& value{ kv.second };

            ASSERT_EQ(reader.ValueOf(key), value);
        }
    }
}

// TODO test that table reader gracefully handles corrupted files
