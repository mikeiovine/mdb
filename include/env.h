#pragma once

#include <memory>
#include <string>

#include "file.h"

namespace mdb {

class Env {
    public:
        virtual ~Env() = default;
        
        static std::shared_ptr<Env> CreateDefault();

        virtual std::unique_ptr<WriteOnlyIO> MakeWriteOnlyIO(std::string filename) const = 0;
        virtual std::unique_ptr<ReadOnlyIO> MakeReadOnlyIO(std::string filename) const = 0;

        virtual void RemoveFile(const std::string& filename) = 0;
};

}
