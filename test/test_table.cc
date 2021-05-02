#include <gtest/gtest.h>

#include "table.h"
#include "util.h"

using namespace mdb;

TEST(TestTable, TestTableWriteMemtable) {
  Options opt{MakeMockOptions()};

  Table table;

  MemTableT memtable{{"1", "10"}, {"2", "10"}, {"3", "10"}};

  table.WriteMemtable(opt, memtable);

  ASSERT_EQ(table.ValueOf("1"), "10");
  ASSERT_EQ(table.ValueOf("2"), "10");
  ASSERT_EQ(table.ValueOf("3"), "10");
  ASSERT_EQ(table.ValueOf("4"), "");
}

/**
 * The table should return the correct value (most recent) after
 * compaction. Only two tables are compacted here.
 */
TEST(TestTable, TestTableCompactionTwoTables) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 2;

  Table table;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  table.WriteMemtable(opt, memtable1);

  MemTableT memtable2{// Deleted key not written.
                      {"1", ""},
                      // New value overrides old one.
                      {"3", "1"}};

  table.WriteMemtable(opt, memtable2);

  table.WaitForOngoingCompactions();

  ASSERT_EQ(table.ValueOf("3"), "1");
  ASSERT_EQ(table.ValueOf("2"), "");
  ASSERT_EQ(table.ValueOf("1"), "");

  ASSERT_EQ(env->files.size(), 1);
}

/**
 * The table should return the correct value (most recent) after
 * compaction. Three tables are compacted here.
 */
TEST(TestTable, TestTableCompactionThreeTables) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 3;

  Table table;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  table.WriteMemtable(opt, memtable1);

  MemTableT memtable2{{"1", ""},
                      // New value overrides old one.
                      {"3", "1"}};

  table.WriteMemtable(opt, memtable2);

  MemTableT memtable3{// New value overrides old one.
                      {"1", "5"},
                      // New key
                      {"4", "1"},
                      // Deleted key
                      {"3", ""}};

  table.WriteMemtable(opt, memtable3);

  table.WaitForOngoingCompactions();

  ASSERT_EQ(table.ValueOf("4"), "1");
  ASSERT_EQ(table.ValueOf("3"), "");
  ASSERT_EQ(table.ValueOf("2"), "");
  ASSERT_EQ(table.ValueOf("1"), "5");

  ASSERT_EQ(env->files.size(), 1);
}

/**
 * The table should return the correct value (most recent) even
 * when no compaction occurs.
 */
TEST(TestTable, TestTableNoCompaction) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 3;

  Table table;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  table.WriteMemtable(opt, memtable1);

  MemTableT memtable2{{"1", ""}, {"3", "1"}};

  table.WriteMemtable(opt, memtable2);

  ASSERT_EQ(table.ValueOf("3"), "1");
  ASSERT_EQ(table.ValueOf("2"), "");
  ASSERT_EQ(table.ValueOf("1"), "");

  ASSERT_EQ(env->files.size(), 2);
}
