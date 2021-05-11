#include "db.h"
#include "options.h"
#include "unit_test_include.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestDB)

/**
 * Put some keys and get their values. No memtable flush happens in this test;
 * everything is in-memory
 */
BOOST_AUTO_TEST_CASE(TestPutAndGetInMemory) {
  Options opt{.path = "./db_e2e_test", .recovery_mode = false};
  DB db{std::move(opt)};

  std::vector<std::pair<std::string, std::string>> key_values{
      {"hello", "world"},
      {"123", "456"},
      {"Hello", "world"},
  };

  for (const auto& kv : key_values) {
    db.Put(kv.first, kv.second);
  }

  for (const auto& kv : key_values) {
    BOOST_REQUIRE_EQUAL(db.Get(kv.first), kv.second);
  }
}

/**
 * Put some keys and get their values. Some keys may be on disk in this test.
 */
BOOST_AUTO_TEST_CASE(TestPutAndGetAfterFlush) {
  Options opt{
      .path = "./db_e2e_test", .recovery_mode = false, .memtable_max_size = 16};
  DB db{std::move(opt)};

  std::vector<std::pair<std::string, std::string>> key_values{
      {"hello", "world"},
      {"123", "456"},
      {"Hello", "world"},
  };

  for (const auto& kv : key_values) {
    db.Put(kv.first, kv.second);
  }

  for (const auto& kv : key_values) {
    BOOST_REQUIRE_EQUAL(db.Get(kv.first), kv.second);
  }
}

/**
 * Put some keys and get their values. In this test, we have keys on disk that
 * are invalidated by newer keys in memory.
 */
BOOST_AUTO_TEST_CASE(TestPutAndGetAfterFlushInMemoryOverwrites) {
  Options opt{
      .path = "./db_e2e_test", .recovery_mode = false, .memtable_max_size = 16};
  DB db{std::move(opt)};

  std::vector<std::pair<std::string, std::string>> key_values{
      {"hello", "world"},
      // Big key-value forces a flush
      {"111111111111111111", "1111111111111111111111"},
      // Not flushed
      {"hello", "overwrite"},
  };

  for (const auto& kv : key_values) {
    db.Put(kv.first, kv.second);
  }

  BOOST_REQUIRE_EQUAL(db.Get("hello"), "overwrite");
}

/**
 * Put some keys and get their values. In this test, we have trigger a
 * compaction that overwrites some keys.
 */
BOOST_AUTO_TEST_CASE(TestPutAndGetAfterCompaction) {
  Options opt{.path = "./db_e2e_test",
              .recovery_mode = false,
              .memtable_max_size = 16,
              .trigger_compaction_at = 2};
  DB db{std::move(opt)};

  std::vector<std::pair<std::string, std::string>> key_values{
      // First flush
      {"hello", "world"},
      {"somekey", "somevalue"},
      // Second flush
      {"hello", "overwrite"},
      {"anotherkey", "anothervalue"},
  };

  for (const auto& kv : key_values) {
    db.Put(kv.first, kv.second);
  }

  db.WaitForOngoingCompactions();

  BOOST_REQUIRE_EQUAL(db.Get("hello"), "overwrite");
  BOOST_REQUIRE_EQUAL(db.Get("somekey"), "somevalue");
  BOOST_REQUIRE_EQUAL(db.Get("anotherkey"), "anothervalue");
}

/**
 * Test that the key/value pairs can be recovered when starting in recovery
 * mode.
 */
BOOST_AUTO_TEST_CASE(TestRecoveryMode) {
  Options opt{
      .path = "./db_e2e_test", .recovery_mode = false, .memtable_max_size = 16};

  std::vector<std::pair<std::string, std::string>> key_values{
      // First flush
      {"hello", "world"},
      {"somekey", "somevalue"},
      // Second flush, recovery must preserve order
      {"hello", "overwrite"},
      {"anotherkey", "anothervalue"},
      // Not flushed, must be recovered from log file
      {"inmemory", "key"}};

  {
    DB db{opt};
    for (const auto& kv : key_values) {
      db.Put(kv.first, kv.second);
    }
  }

  opt.recovery_mode = true;
  DB db{std::move(opt)};

  BOOST_REQUIRE_EQUAL(db.Get("hello"), "overwrite");
  BOOST_REQUIRE_EQUAL(db.Get("somekey"), "somevalue");
  BOOST_REQUIRE_EQUAL(db.Get("anotherkey"), "anothervalue");
  BOOST_REQUIRE_EQUAL(db.Get("inmemory"), "key");
}

BOOST_AUTO_TEST_SUITE_END()
