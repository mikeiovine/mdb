#include <gtest/gtest.h>

#include "helpers.h"
#include "options.h"
#include "util.h"

using namespace mdb;

TEST(TestHelpers, TestAddStringToWritable) {
  std::vector<char> buf;

  std::string s1{"somestring"};
  std::string s2{"another"};

  util::AddStringToWritable(s1, buf);
  util::AddStringToWritable(s2, buf);

  ASSERT_EQ(buf.size(), 2 * sizeof(size_t) + s1.size() + s2.size());

  size_t buf_s1size{ReadSizeT(buf, 0)};
  std::string buf_s1{ReadString(buf, sizeof(size_t), buf_s1size)};

  size_t buf_s2size{ReadSizeT(buf, sizeof(size_t) + buf_s1size)};
  std::string buf_s2{
      ReadString(buf, 2 * sizeof(size_t) + buf_s1size, buf_s2size)};

  ASSERT_EQ(buf_s1size, s1.size());
  ASSERT_EQ(buf_s1, s1);
  ASSERT_EQ(buf_s2size, s2.size());
  ASSERT_EQ(buf_s2, s2);
}

TEST(TestHelpers, TestLogFileName) {
  Options opt1{.path = "./"};
  Options opt2{.path = "/another/path/"};
  Options opt3{.path = "/forgot/slash"};

  std::string logfile_1{util::LogFileName(opt1, 10)};
  std::string logfile_2{util::LogFileName(opt2, 5)};
  std::string logfile_3{util::LogFileName(opt3, 100)};

  ASSERT_EQ(logfile_1, "./log10.dat");
  ASSERT_EQ(logfile_2, "/another/path/log5.dat");
  ASSERT_EQ(logfile_3, "/forgot/slash/log100.dat");
}

TEST(TestHelpers, TestTableFileName) {
  Options opt1{.path = ""};
  Options opt2{.path = "/path/"};
  Options opt3{.path = "/path"};

  std::string logfile_1{util::TableFileName(opt1, 1)};
  std::string logfile_2{util::TableFileName(opt2, 2)};
  std::string logfile_3{util::TableFileName(opt3, 3)};

  ASSERT_EQ(logfile_1, "table1.mdb");
  ASSERT_EQ(logfile_2, "/path/table2.mdb");
  ASSERT_EQ(logfile_3, "/path/table3.mdb");
}
