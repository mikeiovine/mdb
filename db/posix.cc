#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <system_error>
#include <vector>

#include "env.h"

namespace mdb {
namespace {

void ThrowIfError(int ret) {
  if (ret == -1) {
    throw std::system_error(errno, std::generic_category());
  }
}

class PosixWriteOnlyFile : public WriteOnlyIO {
 public:
  PosixWriteOnlyFile(std::string filename)
      : fd_{::open(filename.c_str(), O_TRUNC | O_APPEND | O_WRONLY | O_CREAT,
                   0644)},
        filename_{std::move(filename)} {
    ThrowIfError(fd_);
  }

  PosixWriteOnlyFile(const PosixWriteOnlyFile&) = delete;
  PosixWriteOnlyFile& operator=(const WriteOnlyIO&) = delete;

  PosixWriteOnlyFile(PosixWriteOnlyFile&&) = delete;
  PosixWriteOnlyFile& operator=(PosixWriteOnlyFile&&) = delete;

  ~PosixWriteOnlyFile() override {
    if (!closed_ && fd_ != -1) {
      // Ignore errors
      ::close(fd_);
    }
  }

  void Write(const char* data, size_t size) override {
    ThrowIfError(::write(fd_, data, size));
  }

  void Sync() override { ThrowIfError(::fsync(fd_)); }

  void Close() override {
    if (!closed_) {
      closed_ = true;
      ThrowIfError(::close(fd_));
    }
  }

  std::string GetFileName() const noexcept override { return filename_; }

  int GetID() const noexcept override { return fd_; }

 private:
  const int fd_;
  bool closed_{false};

  std::string filename_;
};

class PosixReadOnlyFile : public ReadOnlyIO {
 public:
  PosixReadOnlyFile(std::string filename)
      : fd_{::open(filename.c_str(), O_RDONLY)},
        filename_{std::move(filename)} {}

  ~PosixReadOnlyFile() override {
    if (!closed_ && fd_ != -1) {
      ::close(fd_);
    }
  }

  size_t Read(char* output, size_t size, size_t offset) override {
    if (!closed_) {
      auto bytes_read{::pread(fd_, output, size, offset)};
      ThrowIfError(bytes_read);
      return bytes_read;
    }
    return 0;
  }

  size_t ReadNoExcept(char* output, size_t size,
                      size_t offset) noexcept override {
    try {
      return Read(output, size, offset);
    } catch (const std::system_error&) {
      return 0;
    }
  }

  void Close() override {
    if (!closed_) {
      closed_ = true;
      ThrowIfError(::close(fd_));
    }
  }

  std::string GetFileName() const noexcept override { return filename_; }

  int GetID() const noexcept override { return fd_; }

  size_t Size() const override {
    struct stat s;
    ThrowIfError(::fstat(fd_, &s));
    return s.st_size;
  }

 private:
  const int fd_;
  bool closed_{false};
  std::string filename_;
};

class PosixEnv : public Env {
 public:
  std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(
      std::string filename) const override {
    return std::make_unique<PosixWriteOnlyFile>(std::move(filename));
  }

  std::unique_ptr<ReadOnlyIO> MakeReadOnlyIO(
      std::string filename) const override {
    return std::make_unique<PosixReadOnlyFile>(std::move(filename));
  }

  void RemoveFile(const std::string& file) override {
    ThrowIfError(::remove(file.c_str()));
  }
};

}  // namespace

std::shared_ptr<Env> Env::CreateDefault() {
  return std::make_shared<PosixEnv>();
}

}  // namespace mdb
