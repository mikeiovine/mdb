#include "log_reader.h"

#include <cassert>
#include <vector>

#include "helpers.h"

namespace mdb {

LogReader::LogReader(int log_number, const Options& options)
    : LogReader(
          options.env->MakeReadOnlyIO(util::LogFileName(options, log_number))) {
}

LogReader::LogReader(std::unique_ptr<ReadOnlyIO>&& file)
    : file_{std::move(file)} {
  assert(file_ != nullptr);
}

std::optional<std::string> LogReader::ReadNextString() {
  assert(file_ != nullptr);

  std::vector<char> buf;
  buf.reserve(sizeof(size_t));

  if (file_->ReadNoExcept(buf.data(), sizeof(size_t), pos_) != sizeof(size_t)) {
    return std::nullopt;
  }
  pos_ += sizeof(size_t);

  size_t str_size{*reinterpret_cast<size_t*>(buf.data())};

  if (str_size == 0) {
    return "";
  }

  try {
    buf.reserve(str_size);
  } catch (const std::bad_alloc&) {
    return std::nullopt;
  }

  if (file_->ReadNoExcept(buf.data(), str_size, pos_) != str_size) {
    return std::nullopt;
  }
  pos_ += str_size;

  return std::string{buf.data(), str_size};
}

MemTableT LogReader::ReadMemTable() {
  MemTableT memtable;

  auto key = ReadNextString();
  auto value = ReadNextString();

  while (key && value) {
    if (value->size() > 0) {
      memtable.insert_or_assign(key.value(), value.value());
    } else {
      memtable.erase(key.value());
    }

    key = ReadNextString();
    if (key) {
      value = ReadNextString();
    }
  }

  return memtable;
}

std::string LogReader::GetFileName() const noexcept {
  assert(file_ != nullptr);
  return file_->GetFileName();
}

}  // namespace mdb
