cmake_minimum_required(VERSION 3.10)

project(mdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(
    mdb_lib 
    db/posix.cc
    db/log_writer.cc
    db/log_reader.cc
    db/helpers.cc
    db/table_reader.cc
)

add_executable(
    mdb 
    db/main.cc
)

target_link_libraries(
    mdb
    mdb_lib
)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/master.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(db include)

add_executable(
    mdb_test
    test/test_log_writer.cc
    test/test_log_reader.cc
    test/test_table_writer.cc
    test/test_table_reader.cc
)

target_link_libraries(
    mdb_test
    mdb_lib
    gtest
    gtest_main
)

include(GoogleTest)
gtest_discover_tests(mdb_test)
