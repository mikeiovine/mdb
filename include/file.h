#pragma once

#include <string>

namespace mdb {

class WriteOnlyIO {
    public:
        WriteOnlyIO() = default;

        WriteOnlyIO(const WriteOnlyIO&) = delete;
        WriteOnlyIO& operator=(const WriteOnlyIO&) = delete;

        WriteOnlyIO(WriteOnlyIO&&) = delete;
        WriteOnlyIO& operator=(WriteOnlyIO&&) = delete;

        virtual ~WriteOnlyIO() = default;

        virtual void Write(const char * data, size_t size) = 0;
        virtual void Sync() = 0;
        virtual void Close() = 0;

        virtual std::string GetFileName() const noexcept { return ""; }
};

class ReadOnlyIO {
    public:
        ReadOnlyIO() = default;

        ReadOnlyIO(const ReadOnlyIO&) = delete;
        ReadOnlyIO& operator=(const ReadOnlyIO&) = delete;

        ReadOnlyIO(ReadOnlyIO&&) = delete;
        ReadOnlyIO& operator=(ReadOnlyIO&&) = delete;

        virtual ~ReadOnlyIO() = default;

        virtual size_t Read(char * output, size_t size) = 0;

        virtual size_t ReadNoExcept(char * output, size_t size) noexcept {
            return Read(output, size);
        }

        virtual void Close() = 0;

        virtual void Seek(size_t offset) = 0;

        virtual std::string GetFileName() const noexcept { return ""; }
};

} // namespace mdb
