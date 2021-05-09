#include <boost/log/trivial.hpp>
#include <boost/program_options.hpp>
#include <filesystem>
#include <iostream>
#include <string>

#include "db.h"
#include "options.h"

using namespace mdb;

namespace po = boost::program_options;

using PathT = std::filesystem::path;

int main(int argc, char* argv[]) {
  po::options_description desc{"Usage"};
  desc.add_options()("help", "Show this help message")(
      "path", po::value<PathT>()->default_value("./db_files"),
      "Where to store the files that MDB will generate.");

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

  PathT path{std::filesystem::canonical(vm["path"].as<PathT>())};
  BOOST_LOG_TRIVIAL(info) << "Started MDB. Writing files to " << path;

  Options opt{.path = path};
  DB db(opt);

  return 0;
}
