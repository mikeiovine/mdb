#pragma once

#include "env.h"

#include <memory>

namespace mdb {

struct Options {
    Options(bool write_sync_ = false) : 
        env{ Env::CreateDefault() }, 
        write_sync{ write_sync_ } {}

    Options(
        std::unique_ptr<Env> env_, 
        bool write_sync_ = false) : 
        env{ std::move(env_) },
        write_sync{ write_sync_ } {}

    std::unique_ptr<Env> env;
    
    // If true, force sync with disk (e.g. via a call to fsync())
    // after every write. 
    const bool write_sync;

    size_t block_size{ 4096 };
};

}
