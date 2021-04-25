#include <gtest/gtest.h>

#include "table_reader.h"
#include "util.h"

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
    res.insert(res.end(), block.data.cbegin(), block.data.cend());
  }

  return res;
}

IndexT ConstructIndex(const std::vector<char>& buf) {
  size_t pos{0};
  IndexT idx;

  while (pos < buf.size()) {
    size_t block_size{ReadSizeT(buf, pos)};
    pos += sizeof(size_t);

    size_t first_key_size{ReadSizeT(buf, pos)};
    std::string first_key{
        ReadString(buf, pos + sizeof(size_t), first_key_size)};

    idx[first_key] = pos - sizeof(size_t);
    pos += block_size;
  }

  return idx;
}

TEST(TestUncompressedTableReader, TestTableReaderFindsCorrectKeys) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  for (const auto& kv_map : key_values) {
    for (const auto& kv : kv_map) {
      const auto& key{kv.first};
      const auto& value{kv.second};

      ASSERT_EQ(reader.ValueOf(key), value);
    }
  }
}

TEST(TestUncompressedTableReader, TestTableReaderIterCorrectOrder) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  auto it{reader.Begin()};

  for (const auto& kv_map : key_values) {
    for (const auto& kv : kv_map) {
      const auto& key{kv.first};
      const auto& value{kv.second};

      ASSERT_EQ(it->first, key);
      ASSERT_EQ(it->second, value);
      ++it;
    }
  }
}

TEST(TestUncompressedTableReader, TestTableIterEq) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  auto it1{reader.Begin()};
  auto it2{reader.Begin()};

  ASSERT_EQ(it1, it2);
  it2++;

  ASSERT_NE(it1, it2);
  it1++;

  ASSERT_EQ(it1, it2);

  ASSERT_EQ(reader.End(), reader.End());
  ASSERT_NE(reader.Begin(), reader.End());
}

TEST(TestUncompressedTableReader, TestTableIterEmpty) {
  std::vector<char> buf;
  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};
  ASSERT_EQ(reader.Begin(), reader.End());
}

TEST(TestUncompressedTableReader, TestTableIterSTL) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}, {"xyz", "abc"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  std::vector<std::pair<std::string, std::string>> output;
  std::copy(reader.Begin(), reader.End(), std::back_inserter(output));

  ASSERT_EQ(output.size(), 3);
  size_t i{0};

  for (auto it = reader.Begin(); it != reader.End(); it++) {
    ASSERT_EQ(*it, output[i]);
    ++i;
  }

  ASSERT_EQ(std::distance(reader.Begin(), reader.End()), 3);
}

TEST(TestUncompressedTableReader, TestTableIterIncAndDeref) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  auto it{reader.Begin()};

  // Equivalent to:
  // valu_type kv = *it;
  // ++it;
  // return kv;

  auto kv{*it++};
  ASSERT_EQ(kv.first, "a");
  ASSERT_EQ(kv.second, "helloworld");

  ASSERT_EQ(it->first, "abc");
  ASSERT_EQ(it->second, "def");

  (void)it++;

  ASSERT_EQ(it, reader.End());
}

TEST(TestUncompressedTableReader, TestTableCorruptionHugeBlockSize) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  *reinterpret_cast<size_t*>(buf.data()) = 100000000000000;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  ASSERT_THROW(reader.ValueOf("abc"), std::system_error);
}

TEST(TestUncompressedTableReader, TestTableCorruptionHugeKeySize) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  *reinterpret_cast<size_t*>(buf.data() + sizeof(size_t)) = 10000;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  ASSERT_THROW(reader.ValueOf("abc"), std::system_error);
}

TEST(TestUncompressedTableReader, TestTableCorruptionHugeValueSize) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto& kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks)};

  auto index{ConstructIndex(buf)};
  *reinterpret_cast<size_t*>(buf.data() + 2 * sizeof(size_t) + sizeof("abc")) =
      100000000000000;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  ASSERT_THROW(reader.ValueOf("abc"), std::system_error);
}
