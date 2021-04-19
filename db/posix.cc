#include "env.h"

#include <system_error>
#include <vector>

#include <unistd.h>
#include <fcntl.h>

namespace mdb {
namespace {

void ThrowIfError(int ret) {
    if (ret == -1) {
        throw std::system_error(errno, std::generic_category());
    }
}


class PosixWriteOnlyFile : public WriteOnlyIO {
    public:
        PosixWriteOnlyFile(const std::string& filename) :
            fd_{ ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644) } {
            
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

        void Write(const char * data, size_t size) override {
            ThrowIfError(::write(fd_, data, size));
        }

        void Sync() override {
            ThrowIfError(::fsync(fd_));
        }

        void Close() override {
            if (!closed_) {
                closed_ = true;
                ThrowIfError(::close(fd_));
            }
        }

    private:
        const int fd_;
        bool closed_{ false };
};


class PosixReadOnlyFile : public ReadOnlyIO {
    public:
        PosixReadOnlyFile(const std::string& filename) :
            fd_{ ::open(filename.c_str(), O_RDONLY) } {}

        ~PosixReadOnlyFile() override {
            if (!closed_ && fd_ != -1) {
                ::close(fd_);
            }
        }

        size_t Read(char * output, size_t size) override {
            if (!closed_) {
                auto bytes_read{ ::read(fd_, output, size) };
                ThrowIfError(bytes_read);
                return bytes_read;
            }
            return 0;
        }

        size_t ReadNoExcept(char * output, size_t size) noexcept override {
            try {
                return Read(output, size);
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

        void Seek(size_t offset) override {
            if (!closed_) {
                ThrowIfError(::lseek(fd_, static_cast<off_t>(offset), SEEK_SET));
            }
        }
        
    private:
        const int fd_;
        bool closed_{ false };    
};

class PosixEnv : public Env {
    public:
        std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(const std::string& filename) override {
            return std::make_unique<PosixWriteOnlyFile>(filename); 
        }
};

} // namespace 

std::unique_ptr<Env> Env::CreateDefault() {
    return std::make_unique<PosixEnv>();
}

} // namespace mdb
