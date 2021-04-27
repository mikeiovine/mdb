#include <gtest/gtest.h>

#include <iostream>

#include "table_reader.h"
#include "table_writer.h"
#include "util.h"

using namespace mdb;

/**
 * Test that the table reader can read what the table writer outputs.
 */
TEST(TestTableIntegration, TestUncompressedTableIntegration) {
  std::vector<char> output;
  auto io{std::make_unique<WriteOnlyIOMock>(output)};

  MemTableT memtable{{"a", "b"}, {"c", "d"}, {"e", ""}};

  UncompressedTableWriter writer{std::move(io), false, 16 + 2 * sizeof(size_t)};

  writer.WriteMemtable(memtable);
  writer.Add("f", "added_later");
  writer.Flush();

  auto io_read{std::make_unique<ReadOnlyIOMock>(std::move(output))};
  UncompressedTableReader reader{std::move(io_read), writer.GetIndex()};

  for (const auto& pair : memtable) {
    ASSERT_EQ(reader.ValueOf(pair.first), pair.second);
  }

  ASSERT_EQ(reader.ValueOf("f"), "added_later");
}
