cmake_minimum_required(VERSION 3.10)

project(mdb)

include(FetchContent)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "-Wall -Wextra -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(
    mdb_lib 
    db/posix.cc
    db/log_writer.cc
    db/log_reader.cc
    db/helpers.cc
    db/table_reader.cc
    db/table_writer.cc
    db/table_factory.cc
    db/disk_storage_manager.cc
    db/db.cc
)

find_package(Boost REQUIRED COMPONENTS program_options)
if(Boost_FOUND)
    add_executable(
        mdb 
        db/mdb.cc
    )
    target_include_directories(mdb PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(
        mdb
        mdb_lib
        Boost::program_options
    )

    add_executable(
        mdb_bench
        benchmark/run_benchmarks.cc
        benchmark/write_random.cc
        benchmark/read_random.cc
        benchmark/benchmark_util.cc
    )
    target_include_directories(mdb_bench PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(
        mdb_bench
        mdb_lib
        Boost::program_options
    )
endif()



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
    test/test_helpers.cc
    test/test_log_integration.cc
    test/test_table_integration.cc
    test/test_disk_storage_manager.cc
)

target_link_libraries(
    mdb_test
    mdb_lib
    gtest
    gtest_main
)

include(GoogleTest)
gtest_discover_tests(mdb_test)
