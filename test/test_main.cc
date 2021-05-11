#define BOOST_TEST_MODULE MDB Unit Tests
#define BOOST_TEST_NO_MAIN
#include <boost/log/core/core.hpp>
#include <boost/test/unit_test.hpp>

int main(int argc, char* argv[]) {
  boost::log::core::get()->set_logging_enabled(false);
  return boost::unit_test::unit_test_main(&init_unit_test, argc, argv);
}
