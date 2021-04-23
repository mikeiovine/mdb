#pragma once

#include <array>
#include <memory>
#include <vector>

#include "file.h"
#include "options.h"

namespace mdb {

class LogWriter {
 public:
  explicit LogWriter(int log_number, const Options& options);
  LogWriter(std::unique_ptr<WriteOnlyIO> file, bool sync);

  LogWriter(const LogWriter&) = delete;
  LogWriter& operator=(const LogWriter&) = delete;

  LogWriter(LogWriter&&) = default;
  LogWriter& operator=(LogWriter&&) = default;

  ~LogWriter();

  void Add(std::string_view key, std::string_view value);
  void MarkDelete(std::string_view key);

  void FlushBuffer();

  size_t Size() const noexcept;

  std::string GetFileName() const noexcept;

 private:
  static constexpr size_t kBlockSize{512};

  void Append(const std::vector<char>& data);

  void BufferData(const std::vector<char>& data);

  size_t GetSpaceAvail() const noexcept;

  std::unique_ptr<WriteOnlyIO> file_;

  std::array<char, kBlockSize> buf_;

  int buf_pos_{0};

  size_t size_{0};

  bool sync_;
};

}  // namespace mdb
