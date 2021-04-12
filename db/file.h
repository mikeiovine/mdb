#ifndef FILE_H_
#define FILE_H_

#include <string>

namespace mdb {

class WritableFile {
    public:
        WritableFile(const std::string& filename);

        WritableFile(const WritableFile&) = delete;
        WritableFile& operator=(const WritableFile&) = delete; 

        WritableFile(WritableFile&&) = delete;
        WritableFile& operator=(WritableFile&&) = delete;

        ~WritableFile() = default;

        void add(const std::string& key, const std::string& value);

    private:
        int fd_;
        void write(const char * data, size_t size);
};

}

#endif
