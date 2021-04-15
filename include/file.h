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

} // namespace mdb
