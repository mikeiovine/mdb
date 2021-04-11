#ifndef TABLE_BUILDER_H_
#define TABLE_BUILDER_H_

#include <string>
#include <fstream>

namespace mdb {

class TableBuilder {
    public:
        TableBuilder(const std::string& filename);

        TableBuilder(const TableBuilder&) = delete;
        TableBuilder& operator=(const TableBuilder&) = delete; 

        TableBuilder(TableBuilder&&) = delete;
        TableBuilder& operator=(TableBuilder&&) = delete;

        ~TableBuilder() = default;

        void add(const std::string& key, const std::string& value);

    private:
        std::ofstream output_;
};

}

#endif
