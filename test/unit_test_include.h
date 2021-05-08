// This file should be included instead of boost/test/unit_test.hpp in all test
// files.
//
// This is because it injects a template specialization that most test files for
// this project will need. This is a nasty hack, but there is no way around this
// until the boost team adds this feature.
//
// Ref: https://github.com/boostorg/test/issues/309
#pragma once

#include <boost/test/unit_test.hpp>

namespace boost {
namespace test_tools {
namespace tt_detail {

template <class K, class V>
struct print_log_value<std::pair<K, V>> {
  void operator()(std::ostream &os, std::pair<K, V> kv) {
    os << "{" << kv.first << ", " << kv.second << "}";
  }
};

}  // namespace tt_detail
}  // namespace test_tools
}  // namespace boost
