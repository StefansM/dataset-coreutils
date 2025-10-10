#ifndef ARROW_RESULT_H_
#define ARROW_RESULT_H_

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

#endif /* ARROW_RESULT_H_ */
