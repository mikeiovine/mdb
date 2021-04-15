#pragma once

#include <memory>
#include <array>

#include "file.h"

namespace mdb {

class LogWriter {
    public:
        explicit LogWriter(std::unique_ptr<WriteOnlyIO> file);

        void Add(const std::string& key, const std::string& value); 

        static constexpr size_t kBlockSize{ 512 };
        
        static constexpr bool kSyncWrites{ false };
    private:
        void Append(const std::string& data);

        std::unique_ptr<WriteOnlyIO> file_;

        std::array<char, kBlockSize> buf_;

        int bufPos_{ 0 };
};

}
