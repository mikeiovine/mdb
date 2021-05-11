#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <string>

#include "db.h"
#include "options.h"

using namespace mdb;

namespace po = boost::program_options;
namespace keywords = boost::log::keywords;
namespace expr = boost::log::expressions;

using PathT = std::filesystem::path;

int main(int argc, char* argv[]) {
  po::options_description desc{"Usage"};
  desc.add_options()("help", "Show this help message")(
      "path", po::value<PathT>()->default_value("./db_files"),
      "Where to store the files that MDB will "
      "generate.")("recover", po::bool_switch(),
                   "If on, start the database in "
                   "recovery mode (see options.h)");

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << '\n';
    return 1;
  }

  if (vm.count("help")) {
    std::cout << desc << '\n';
    return 1;
  }

  PathT path{vm["path"].as<PathT>()};

  boost::log::add_common_attributes();

  boost::log::add_file_log(
      keywords::file_name = "mdb_%N.log", keywords::auto_flush = true,
      keywords::target = path / "logs",
      keywords::rotation_size = 10 * 1024 * 1024, keywords::max_files = 10,
      keywords::format =
          (expr::stream << expr::attr<unsigned int>("LineID") << ": ["
                        << boost::log::trivial::severity << "] "
                        << expr::format_date_time<boost::posix_time::ptime>(
                               "TimeStamp", "[%m-%d-%Y %H:%M:%S] ")
                        << expr::message));

  Options opt{.path = path, .recovery_mode = vm["recover"].as<bool>()};
  DB db{std::move(opt)};

  return 0;
}
