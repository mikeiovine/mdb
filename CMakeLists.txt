cmake_minimum_required(VERSION 3.10)

project(mdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable(mdb db/file.cpp db/main.cpp)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/master.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
    mdb_test
    test/test_file.cpp
)

target_link_libraries(
    mdb_test
    gtest
    gtest_main
)

include(GoogleTest)
gtest_discover_tests(mdb_test)
