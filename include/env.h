#pragma once

#include <memory>
#include <string>

#include "file.h"

namespace mdb {

class Env {
    public:
        virtual ~Env() = default;
        
        static std::unique_ptr<Env> CreateDefault();

        // TODO more
        virtual std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(const std::string& filename) = 0;
};

}
