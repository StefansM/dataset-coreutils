#ifndef ARROW_RESULT_H_
#define ARROW_RESULT_H_

#include <exception>

struct ArrowException : public std::runtime_error {
    ArrowException(std::string msg) : std::runtime_error(msg) {}
};


template <typename T>
T assign_or_raise(arrow::Result<T> result) {
    if (!result.ok()) {
        throw ArrowException("Error doing Arrow action. " + result.status().ToString());
    }
    return *result;
}

#endif /* ARROW_RESULT_H_ */

