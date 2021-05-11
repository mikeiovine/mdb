cmake_minimum_required(VERSION 3.10)

project(mdb)

include(FetchContent)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "-Wall -Wextra -O3")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Boost 1.76 REQUIRED COMPONENTS program_options unit_test_framework log)
if(Boost_FOUND)
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
    target_include_directories(mdb_lib PUBLIC db include ${Boost_INCLUDE_DIRS})
    target_link_libraries(
        mdb_lib
        Boost::log
    )

    add_executable(
        mdb 
        db/mdb.cc
    )
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
    target_link_libraries(
        mdb_bench
        mdb_lib
        Boost::program_options
    )

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
        test/test_db.cc
        test/test_main.cc
    )
    target_link_libraries(
        mdb_test
        mdb_lib
        Boost::unit_test_framework
    )
endif()
