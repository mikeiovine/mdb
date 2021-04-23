#pragma once

#include <string>
#include <iostream>

#include "types.h"

namespace mdb {

class UncompressedTableReader;

// Table implementations may inherit from this.
class TableIteratorImpl {
    public:
        using ValueType = std::pair<std::string, std::string>;

        TableIteratorImpl() = default;

        TableIteratorImpl(const TableIteratorImpl&) = delete;
        TableIteratorImpl& operator=(const TableIteratorImpl&) = delete;

        TableIteratorImpl(TableIteratorImpl&&) = delete;
        TableIteratorImpl& operator=(TableIteratorImpl&&) = delete;

        virtual ~TableIteratorImpl() = default;

        virtual ValueType& GetValue() = 0;
        virtual bool IsDone() = 0;
        virtual void Next() = 0; 
        virtual size_t Position() const noexcept = 0;

        virtual bool operator==(const TableIteratorImpl& other) = 0; 

        virtual std::shared_ptr<TableIteratorImpl> Clone() = 0;
};

// An STL-compatible iterator that forwards calls to some underlying implementation.
// Often, we want to pass iterators by value - in fact, the STL requires that 
// iterators be copyable. But copying polymorphic objects is dangerous because
// of slicing. Using this class lets us sidestep this issue and provide a copyable
// class that emulates polymorphic behavior.
class TableIterator {
    public:
        using value_type = TableIteratorImpl::ValueType;
        using difference_type = size_t;
        using reference = TableIteratorImpl::ValueType&;
        using pointer = TableIteratorImpl::ValueType*;
        using iterator_category = std::input_iterator_tag;

        TableIterator(std::shared_ptr<TableIteratorImpl> impl) :
            impl_{ std::move(impl) } {}

        TableIterator(const TableIterator& other) :
            impl_{ other.impl_->Clone() } {}

        TableIterator& operator=(const TableIterator& other) {
            impl_ = other.impl_->Clone();
            return *this; 
        }

        TableIterator(TableIterator&& other) = default;

        TableIterator& operator=(TableIterator&& other) = default;

        ~TableIterator() = default;
        
        reference operator*() {
            return impl_->GetValue();
        }

        TableIterator& operator++() {
            impl_->Next();
            return *this;
        }

        TableIterator operator++(int) {
            auto temp{ *this };
            ++(*this);
            return temp;
        }

        friend bool operator==(const TableIterator& lhs, const TableIterator& rhs) {
            return *lhs.impl_ == *rhs.impl_;
        }

        friend bool operator!=(const TableIterator& lhs, const TableIterator& rhs) {
            return !(lhs == rhs);
        }

        pointer operator->() {
            return &impl_->GetValue();
        }

    private:
        std::shared_ptr<TableIteratorImpl> impl_;
};

}
