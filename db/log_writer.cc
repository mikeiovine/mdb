#include <cassert>
#include <system_error>

#include "log_writer.h"

namespace mdb {

namespace {

void AddStringToWritable(const std::string& str, std::vector<char>& writable) {
    size_t str_size{ str.size() };
    char * size_bytes{ reinterpret_cast<char*>(&str_size) };

    writable.insert(
        writable.end(),
        size_bytes,
        size_bytes + sizeof(size_t));

    writable.insert(
        writable.end(),
        str.begin(),
        str.end());
}

} // namespace

LogWriter::LogWriter(std::unique_ptr<WriteOnlyIO> file, bool sync) : 
    file_{ std::move(file) }, sync_{ sync } {

    assert(file_ != nullptr);
}

LogWriter::~LogWriter() {
    // User did not flush before destructing; we still have pending writes
    if (buf_pos_) {
        try {
            FlushBuffer();
        } catch (const std::system_error&) {
            // TODO: Log that something horrible happened
        }
    }
}

void LogWriter::Add(const std::string& key, const std::string& value) {
    assert(key.size() > 0 && value.size() > 0);

    // For the purposes of exception safety, we write everything together.
    // If we wrote key/value sequentially, and an exception occured during
    // the key write, we would leave the log file in a unreadable state!
    std::vector<char> writable_data; 
    writable_data.reserve(key.size() + value.size() + 2 * sizeof(size_t));

    AddStringToWritable(key, writable_data);
    AddStringToWritable(value, writable_data);

    Append(writable_data);
}

void LogWriter::MarkDelete(const std::string& key) {
    assert(key.size() > 0);

    std::vector<char> writable_data;
    writable_data.reserve(key.size() + 2 * sizeof(size_t));

    AddStringToWritable(key, writable_data);
    
    const size_t zero{ 0 };
    const char * zero_size_bytes{ reinterpret_cast<const char*>(&zero) };

    writable_data.insert(
        writable_data.end(),
        zero_size_bytes,
        zero_size_bytes + sizeof(size_t));

    Append(writable_data);
}

size_t LogWriter::GetSpaceAvail() const noexcept {
    return kBlockSize - buf_pos_;
}

void LogWriter::FlushBuffer() {
    if (buf_pos_) {
        // If syncing is on, writes should always happen instantly.
        assert(!sync_);
        file_->Write(buf_.data(), buf_pos_);
        buf_pos_ = 0;
    }
}

void LogWriter::BufferData(const std::vector<char>& data) {
    assert(file_ != nullptr);

    if (data.size() > kBlockSize) {
        file_->Write(data.data(), data.size());
    } else {
        if (data.size() > kBlockSize - buf_pos_) {
           FlushBuffer();
        }
        std::copy(data.cbegin(), data.cend(), buf_.begin() + buf_pos_);
        buf_pos_ += data.size();
    }
}

void LogWriter::Append(const std::vector<char>& data) {
    assert(file_ != nullptr);
   
    // Syncing is on, always write
    if (sync_) {
        file_->Write(data.data(), data.size());
        file_->Sync();
    } else {
        BufferData(data);
    }
}

} // namespace mdb