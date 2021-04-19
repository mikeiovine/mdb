#pragma once

#include <string>

namespace mdb {

class WriteOnlyIO {
    public:
        virtual ~WriteOnlyIO() = default;

        virtual void Write(const char * data, size_t size) = 0;
        virtual void Sync() = 0;
        virtual void Close() = 0;
};

class ReadOnlyIO {
    public:
        virtual ~ReadOnlyIO() = default;

        virtual size_t Read(char * output, size_t size) = 0;

        virtual size_t ReadNoExcept(char * output, size_t size) noexcept {
            return Read(output, size);
        }

        virtual void Close() = 0;

        virtual void Seek(size_t offset) = 0;
};

} // namespace mdb
