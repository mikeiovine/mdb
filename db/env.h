#pragma once

#include <memory>
#include <string>

#include "file.h"

namespace mdb {

class Env {
    public:
        virtual ~Env() = default;

        // TODO more
        virtual std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(const std::string& filename) = 0;

    protected:
        Env() = default;
};

}
