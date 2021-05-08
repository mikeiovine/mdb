#include <boost/test/unit_test.hpp>

#include "helpers.h"
#include "options.h"
#include "util.h"

using namespace mdb;

BOOST_AUTO_TEST_SUITE(TestHelpers)

BOOST_AUTO_TEST_CASE(TestAddStringToWritable) {
  std::vector<char> buf;

  std::string s1{"somestring"};
  std::string s2{"another"};

  util::AddStringToWritable(s1, buf);
  util::AddStringToWritable(s2, buf);

  BOOST_REQUIRE_EQUAL(buf.size(), 2 * sizeof(size_t) + s1.size() + s2.size());

  size_t buf_s1size{ReadSizeT(buf, 0)};
  std::string buf_s1{ReadString(buf, sizeof(size_t), buf_s1size)};

  size_t buf_s2size{ReadSizeT(buf, sizeof(size_t) + buf_s1size)};
  std::string buf_s2{
      ReadString(buf, 2 * sizeof(size_t) + buf_s1size, buf_s2size)};

  BOOST_REQUIRE_EQUAL(buf_s1size, s1.size());
  BOOST_REQUIRE_EQUAL(buf_s1, s1);
  BOOST_REQUIRE_EQUAL(buf_s2size, s2.size());
  BOOST_REQUIRE_EQUAL(buf_s2, s2);
}

BOOST_AUTO_TEST_CASE(TestLogFileName) {
  Options opt1{.path = "./"};
  Options opt2{.path = "/another/path/"};
  Options opt3{.path = "/forgot/slash"};

  std::string logfile_1{util::LogFileName(opt1, 10)};
  std::string logfile_2{util::LogFileName(opt2, 5)};
  std::string logfile_3{util::LogFileName(opt3, 100)};

  BOOST_REQUIRE_EQUAL(logfile_1, "./log10.dat");
  BOOST_REQUIRE_EQUAL(logfile_2, "/another/path/log5.dat");
  BOOST_REQUIRE_EQUAL(logfile_3, "/forgot/slash/log100.dat");
}

BOOST_AUTO_TEST_CASE(TestTableFileName) {
  Options opt1{.path = ""};
  Options opt2{.path = "/path/"};
  Options opt3{.path = "/path"};

  std::string logfile_1{util::TableFileName(opt1, 1)};
  std::string logfile_2{util::TableFileName(opt2, 2)};
  std::string logfile_3{util::TableFileName(opt3, 3)};

  BOOST_REQUIRE_EQUAL(logfile_1, "table1.mdb");
  BOOST_REQUIRE_EQUAL(logfile_2, "/path/table2.mdb");
  BOOST_REQUIRE_EQUAL(logfile_3, "/path/table3.mdb");
}

BOOST_AUTO_TEST_SUITE_END()
