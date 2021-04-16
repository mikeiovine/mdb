#pragma once

#include <memory>
#include <vector>
#include <array>

#include "file.h"

namespace mdb {

class LogWriter {
    public:
        explicit LogWriter(std::unique_ptr<WriteOnlyIO> file, bool sync);

        LogWriter(const LogWriter&) = delete;
        LogWriter& operator=(const LogWriter&) = delete;

        LogWriter(LogWriter&&) = default;
        LogWriter& operator=(LogWriter&&) = default;

        ~LogWriter();

        void Add(const std::string& key, const std::string& value); 
        void MarkDelete(const std::string& key);

        void FlushBuffer();

    private:
        static constexpr size_t kBlockSize{ 512 };

        void Append(const std::vector<char>& data);

        void BufferData(const std::vector<char>& data);

        size_t GetSpaceAvail() const noexcept;

        std::unique_ptr<WriteOnlyIO> file_;

        std::array<char, kBlockSize> buf_;

        int buf_pos_{ 0 };
        bool sync_;
};

}
