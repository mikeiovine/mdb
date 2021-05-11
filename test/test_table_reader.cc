#include "table_reader.h"
#include "unit_test_include.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestTableReader)

struct BlockT {
  size_t size;
  std::vector<char> data;
};

BlockT ConstructBlock(const std::map<std::string, std::string> &pairs) {
  BlockT block;
  WriteSizeT(block.data, 0);

  for (const auto &kv : pairs) {
    WriteSizeT(block.data, kv.first.size());
    WriteString(block.data, kv.first);

    WriteSizeT(block.data, kv.second.size());
    WriteString(block.data, kv.second);
  }

  block.size = block.data.size() - sizeof(size_t);
  *reinterpret_cast<size_t *>(block.data.data()) = block.size;

  return block;
}

std::vector<char> ConstructTable(const std::vector<BlockT> blocks,
                                 size_t level) {
  std::vector<char> res;
  WriteSizeT(res, level);

  for (const auto &block : blocks) {
    res.insert(res.end(), block.data.cbegin(), block.data.cend());
  }

  return res;
}

IndexT ConstructIndex(const std::vector<char> &buf) {
  size_t pos{sizeof(size_t)};
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

BOOST_AUTO_TEST_CASE(TestTableReaderFindsCorrectKeys) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  for (const auto &kv_map : key_values) {
    for (const auto &kv : kv_map) {
      const auto &key{kv.first};
      const auto &value{kv.second};

      BOOST_REQUIRE(reader.ValueOf(key) == value);
    }
  }
}

BOOST_AUTO_TEST_CASE(TestTableReaderIterCorrectOrder) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  auto it{reader.Begin()};

  for (const auto &kv_map : key_values) {
    for (const auto &kv : kv_map) {
      const auto &key{kv.first};
      const auto &value{kv.second};

      BOOST_REQUIRE_EQUAL(it->first, key);
      BOOST_REQUIRE_EQUAL(it->second, value);
      ++it;
    }
  }
}

BOOST_AUTO_TEST_CASE(TestTableIterEq) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  auto it1{reader.Begin()};
  auto it2{reader.Begin()};

  BOOST_REQUIRE(it1 == it2);
  it2++;

  BOOST_REQUIRE(it1 != it2);
  it1++;

  BOOST_REQUIRE(it1 == it2);

  BOOST_REQUIRE(reader.End() == reader.End());
  BOOST_REQUIRE(reader.Begin() != reader.End());
}

BOOST_AUTO_TEST_CASE(TestTableIterEmpty) {
  std::vector<char> buf;
  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};
  BOOST_REQUIRE(reader.Begin() == reader.End());
}

BOOST_AUTO_TEST_CASE(TestTableIterSTL) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}, {"xyz", "abc"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  std::vector<std::pair<std::string, std::string>> output;
  std::copy(reader.Begin(), reader.End(), std::back_inserter(output));

  size_t expected_size{3};
  BOOST_REQUIRE_EQUAL(output.size(), expected_size);
  size_t i{0};

  for (auto it = reader.Begin(); it != reader.End(); it++) {
    BOOST_REQUIRE(*it == output[i]);
    ++i;
  }

  BOOST_REQUIRE_EQUAL(std::distance(reader.Begin(), reader.End()),
                      expected_size);
}

BOOST_AUTO_TEST_CASE(TestTableIterIncAndDeref) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  auto it{reader.Begin()};

  // Equivalent to:
  // valu_type kv = *it;
  // ++it;
  // return kv;

  auto kv{*it++};
  BOOST_REQUIRE_EQUAL(kv.first, "a");
  BOOST_REQUIRE_EQUAL(kv.second, "helloworld");

  BOOST_REQUIRE_EQUAL(it->first, "abc");
  BOOST_REQUIRE_EQUAL(it->second, "def");

  (void)it++;

  BOOST_REQUIRE(it == reader.End());
}

BOOST_AUTO_TEST_CASE(TestTableCorruptionHugeBlockSize) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  *reinterpret_cast<size_t *>(buf.data() + sizeof(size_t)) = 100000000000000;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  BOOST_REQUIRE_THROW(reader.ValueOf("abc"), std::system_error);
}

BOOST_AUTO_TEST_CASE(TestTableCorruptionHugeKeySize) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  *reinterpret_cast<size_t *>(buf.data() + sizeof(size_t)) = 10000;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  BOOST_REQUIRE_THROW(reader.ValueOf("abc"), std::system_error);
}

BOOST_AUTO_TEST_CASE(TestTableCorruptionHugeValueSize) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  *reinterpret_cast<size_t *>(buf.data() + 3 * sizeof(size_t) + sizeof("abc")) =
      100000000000000;
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  BOOST_REQUIRE_THROW(reader.ValueOf("abc"), std::system_error);
}

/**
 * The table reader differentiates between values that are not
 * found and values that are deleted. Here, we test that
 * ValueOf returns the empty string (NOT std::nullopt) if the key
 * is explicitly deleted in the table.
 */
BOOST_AUTO_TEST_CASE(TestDeletedValuesAreEmpty) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", ""}, {"a", ""}}, {{"xyz", ""}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  for (const auto &kv_map : key_values) {
    for (const auto &kv : kv_map) {
      const auto &key{kv.first};
      const auto &value{kv.second};

      BOOST_REQUIRE(reader.ValueOf(key) == value);
    }
  }
}

/**
 * The table reader differentiates between values that are not
 * found and values that are deleted. Here, we test that
 * ValueOf returns the std::nullopt if the key is not found in
 * the table.
 */
BOOST_AUTO_TEST_CASE(TestNonexistantValuesAreNullopt) {
  std::vector<std::map<std::string, std::string>> key_values{
      {{"ba", ""}, {"bb", ""}}, {{"xyz", ""}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, 0)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  // Don't search any block
  BOOST_REQUIRE(reader.ValueOf("a") == std::nullopt);

  // Search the first block
  BOOST_REQUIRE(reader.ValueOf("bc") == std::nullopt);

  // Search the last block
  BOOST_REQUIRE(reader.ValueOf("zzz") == std::nullopt);
}

/**
 * Test that GetLevel returns the number stored at the start of the file
 */
BOOST_AUTO_TEST_CASE(TestGetLevel) {
  size_t level{42};
  std::vector<std::map<std::string, std::string>> key_values{
      {{"key", "value"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, level)};

  auto index{ConstructIndex(buf)};
  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io), std::move(index))};

  BOOST_REQUIRE_EQUAL(reader.GetLevel(), level);
}

/**
 * Test that the LogReader can recover the correct index/level given only
 * an on-disk file
 */
BOOST_AUTO_TEST_CASE(TestConstructFromFileOnly) {
  size_t level{42};
  std::vector<std::map<std::string, std::string>> key_values{
      {{"abc", "def"}, {"a", "helloworld"}},
      {{"b12", "123451251512"}, {"bbb", "bbbbbbbbbbbbbbbbbbb"}},
      {{"xyz", "hello"}}};

  std::vector<BlockT> blocks;
  for (const auto &kv_map : key_values) {
    blocks.push_back(ConstructBlock(kv_map));
  }

  std::vector<char> buf{ConstructTable(blocks, level)};

  auto io{std::make_unique<ReadOnlyIOMock>(std::move(buf))};

  auto reader{UncompressedTableReader(std::move(io))};

  BOOST_REQUIRE_EQUAL(reader.GetLevel(), level);

  for (const auto &kv_map : key_values) {
    for (const auto &kv : kv_map) {
      const auto &key{kv.first};
      const auto &value{kv.second};

      BOOST_REQUIRE(reader.ValueOf(key) == value);
    }
  }
}

BOOST_AUTO_TEST_SUITE_END()
