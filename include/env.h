#pragma once

#include <memory>
#include <string>

#include "file.h"

namespace mdb {

class Env {
    public:
        virtual ~Env() = default;
        
        static std::unique_ptr<Env> CreateDefault();

        virtual std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(const std::string& filename) const = 0;
        virtual std::unique_ptr<ReadOnlyIO> MakeReadOnlyIO(const std::string& filename) const = 0;
};

}
