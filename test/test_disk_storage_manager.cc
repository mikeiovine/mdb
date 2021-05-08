#include <boost/test/unit_test.hpp>

#include "disk_storage_manager.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestDiskStorageManager)

BOOST_AUTO_TEST_CASE(TestDiskStorageManagerWriteMemtable) {
  Options opt{MakeMockOptions()};

  DiskStorageManager storage_manager;

  MemTableT memtable{{"1", "10"}, {"2", "10"}, {"3", "10"}};

  storage_manager.WriteMemtable(opt, memtable);

  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("1"), "10");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("2"), "10");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("3"), "10");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("4"), "");
}

/**
 * The storage manager should return the correct value (most recent) after
 * compaction. Only two memtables are compacted here.
 */
BOOST_AUTO_TEST_CASE(TestDiskStorageManagerCompactionTwoTables) {
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

  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("3"), "1");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("2"), "");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("1"), "");

  size_t expected_num_files{1};
  BOOST_REQUIRE_EQUAL(env->files.size(), expected_num_files);
}

/**
 * The storage manager should return the correct value (most recent) after
 * compaction. Three memtables are compacted here.
 */
BOOST_AUTO_TEST_CASE(TestDiskStorageManagerCompactionThreeTables) {
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

  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("4"), "1");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("3"), "");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("2"), "");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("1"), "5");

  size_t expected_num_files{1};
  BOOST_REQUIRE_EQUAL(env->files.size(), expected_num_files);
}

/**
 * The storage manager should return the correct value (most recent) even
 * when no compaction occurs.
 */
BOOST_AUTO_TEST_CASE(TestDiskStorageManagerNoCompaction) {
  auto env{std::make_shared<EnvMock>()};
  Options opt{.env = env};
  opt.trigger_compaction_at = 3;

  DiskStorageManager storage_manager;

  MemTableT memtable1{{"1", "10"}, {"2", ""}, {"3", "10"}};

  storage_manager.WriteMemtable(opt, memtable1);

  MemTableT memtable2{{"1", ""}, {"3", "1"}};

  storage_manager.WriteMemtable(opt, memtable2);

  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("3"), "1");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("2"), "");
  BOOST_REQUIRE_EQUAL(storage_manager.ValueOf("1"), "");

  size_t expected_num_files{2};
  BOOST_REQUIRE_EQUAL(env->files.size(), expected_num_files);
}

BOOST_AUTO_TEST_SUITE_END()
