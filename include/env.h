#pragma once

#include <filesystem>
#include <memory>
#include <string>

#include "file.h"

namespace mdb {

class Env {
 public:
  Env() = default;

  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;

  Env(Env&&) = delete;
  Env& operator=(Env&&) = delete;

  virtual ~Env() = default;

  static std::shared_ptr<Env> CreateDefault();

  virtual std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(
      std::string filename) const = 0;
  virtual std::unique_ptr<ReadOnlyIO> MakeReadOnlyIO(
      std::string filename) const = 0;

  virtual void RemoveFile(const std::string& filename) = 0;
};

}  // namespace mdb
