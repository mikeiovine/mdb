#include "log_writer.h"

#include <boost/log/trivial.hpp>
#include <cassert>
#include <system_error>

#include "helpers.h"

namespace mdb {

LogWriter::LogWriter() : file_{nullptr}, sync_{false} {}

LogWriter::LogWriter(int log_number, const Options& options)
    : LogWriter(
          options.env->MakeWriteOnlyIO(util::LogFileName(options, log_number)),
          options.write_sync) {}

LogWriter::LogWriter(std::unique_ptr<WriteOnlyIO>&& file, bool sync)
    : file_{std::move(file)}, sync_{sync} {}

LogWriter::~LogWriter() {
  // User did not flush before destructing; we still have pending writes
  if (buf_pos_) {
    try {
      FlushBuffer();
    } catch (const std::system_error&) {
      BOOST_LOG_TRIVIAL(error)
          << "Failed to flush buffer when destructing LogWriter. Some recent "
             "writes may not have made it to disk!";
    }
  }
}

void LogWriter::Add(std::string_view key, std::string_view value) {
  if (file_ == nullptr) {
    BOOST_LOG_TRIVIAL(warning) << "Trying to log data to nonexistent (== "
                                  "nullptr) log file. No action was taken.";
    return;
  }

  // For the purposes of exception safety, we write everything together.
  // If we wrote key/value sequentially, and an exception occured during
  // the key write, we would leave the log file in a unreadable state!
  std::vector<char> writable_data;
  writable_data.reserve(key.size() + value.size() + 2 * sizeof(size_t));

  util::AddStringToWritable(key, writable_data);
  util::AddStringToWritable(value, writable_data);

  Append(writable_data);
}

size_t LogWriter::GetSpaceAvail() const noexcept {
  return kBlockSize - buf_pos_;
}

void LogWriter::FlushBuffer() {
  if (file_ == nullptr) {
    BOOST_LOG_TRIVIAL(warning) << "Trying to flush buffer to nonexistent (== "
                                  "nullptr) log file. No action was taken.";
    return;
  }

  if (buf_pos_) {
    // If syncing is on, writes should always happen instantly.
    assert(!sync_);
    file_->Write(buf_.data(), buf_pos_);
    size_ += buf_pos_;
    buf_pos_ = 0;
  }
}

void LogWriter::BufferData(const std::vector<char>& data) {
  assert(file_ != nullptr);

  if (data.size() > kBlockSize) {
    file_->Write(data.data(), data.size());
  } else {
    if (data.size() > kBlockSize - buf_pos_) {
      FlushBuffer();
    }
    std::copy(data.cbegin(), data.cend(), buf_.begin() + buf_pos_);
    buf_pos_ += data.size();
  }
}

void LogWriter::Append(const std::vector<char>& data) {
  assert(file_ != nullptr);

  // Syncing is on, always write
  if (sync_) {
    file_->Write(data.data(), data.size());
    file_->Sync();
  } else {
    BufferData(data);
  }
}

size_t LogWriter::Size() const noexcept { return size_ + buf_pos_; }

std::string LogWriter::GetFileName() const noexcept {
  if (file_ == nullptr) {
    BOOST_LOG_TRIVIAL(warning)
        << "Trying to get file name of nonexistent (== nullptr) log file.";
    return "";
  }
  return file_->GetFileName();
}

}  // namespace mdb
