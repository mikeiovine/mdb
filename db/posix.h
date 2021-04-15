#pragma once

#include "env.h"
#include "file.h"

#include <memory>

namespace mdb {

class PosixEnv : public Env {
    public:
        std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(const std::string& filename) override;
};

}
