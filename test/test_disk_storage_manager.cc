#include <gtest/gtest.h>

#include "disk_storage_manager.h"
#include "util.h"

using namespace mdb;

TEST(TestDiskStorageManager, TestDiskStorageManagerWriteMemtable) {
  Options opt{MakeMockOptions()};

  DiskStorageManager storage_manager;

  MemTableT memtable{{"1", "10"}, {"2", "10"}, {"3", "10"}};

  storage_manager.WriteMemtable(opt, memtable);

  ASSERT_EQ(storage_manager.ValueOf("1"), "10");
  ASSERT_EQ(storage_manager.ValueOf("2"), "10");
  ASSERT_EQ(storage_manager.ValueOf("3"), "10");
  ASSERT_EQ(storage_manager.ValueOf("4"), "");
}

/**
 * The storage manager should return the correct value (most recent) after
 * compaction. Only two memtables are compacted here.
 */
TEST(TestDiskStorageManager, TestDiskStorageManagerCompactionTwoTables) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 2;

  DiskStorageManager storage_manager;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  storage_manager.WriteMemtable(opt, memtable1);

  MemTableT memtable2{// Deleted key not written.
                      {"1", ""},
                      // New value overrides old one.
                      {"3", "1"}};

  storage_manager.WriteMemtable(opt, memtable2);

  storage_manager.WaitForOngoingCompactions();

  ASSERT_EQ(storage_manager.ValueOf("3"), "1");
  ASSERT_EQ(storage_manager.ValueOf("2"), "");
  ASSERT_EQ(storage_manager.ValueOf("1"), "");

  ASSERT_EQ(env->files.size(), 1);
}

/**
 * The storage manager should return the correct value (most recent) after
 * compaction. Three memtables are compacted here.
 */
TEST(TestDiskStorageManager, TestDiskStorageManagerCompactionThreeTables) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 3;

  DiskStorageManager storage_manager;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  storage_manager.WriteMemtable(opt, memtable1);

  MemTableT memtable2{{"1", ""},
                      // New value overrides old one.
                      {"3", "1"}};

  storage_manager.WriteMemtable(opt, memtable2);

  MemTableT memtable3{// New value overrides old one.
                      {"1", "5"},
                      // New key
                      {"4", "1"},
                      // Deleted key
                      {"3", ""}};

  storage_manager.WriteMemtable(opt, memtable3);

  storage_manager.WaitForOngoingCompactions();

  ASSERT_EQ(storage_manager.ValueOf("4"), "1");
  ASSERT_EQ(storage_manager.ValueOf("3"), "");
  ASSERT_EQ(storage_manager.ValueOf("2"), "");
  ASSERT_EQ(storage_manager.ValueOf("1"), "5");

  ASSERT_EQ(env->files.size(), 1);
}

/**
 * The storage manager should return the correct value (most recent) even
 * when no compaction occurs.
 */
TEST(TestDiskStorageManager, TestDiskStorageManagerNoCompaction) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 3;

  DiskStorageManager storage_manager;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  storage_manager.WriteMemtable(opt, memtable1);

  MemTableT memtable2{{"1", ""}, {"3", "1"}};

  storage_manager.WriteMemtable(opt, memtable2);

  ASSERT_EQ(storage_manager.ValueOf("3"), "1");
  ASSERT_EQ(storage_manager.ValueOf("2"), "");
  ASSERT_EQ(storage_manager.ValueOf("1"), "");

  ASSERT_EQ(env->files.size(), 2);
}
