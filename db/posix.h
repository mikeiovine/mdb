#pragma once

#include "env.h"
#include "file.h"

#include <memory>

namespace mdb {

std::unique_ptr<Env> CreateEnv();

}
