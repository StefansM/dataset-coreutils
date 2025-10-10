#pragma once

#include <arrow/result.h>
#include <string>

struct ArrowException final : std::runtime_error {
    explicit ArrowException(const std::string &msg) : std::runtime_error(msg) {}
};


template<typename T>
T assign_or_raise(arrow::Result<T> result) {
    if (!result.ok()) {
        throw ArrowException("Error doing Arrow action. " + result.status().ToString());
    }
    return *result;
}
