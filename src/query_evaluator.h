#pragma once

#include <functional>
#include <memory>

#include <arrow/api.h>

class Writer;
class OverallQueryPlan;
class AliasGenerator;

struct DuckDbException final : std::runtime_error {
    explicit DuckDbException(
        const std::string &msg
    );
};

enum class ExitStatus : std::int8_t {
    SUCCESS = 0,
    QUERY_GENERATION_ERROR = 1,
    EXECUTION_ERROR = 2,
    PROGRAMMING_ERROR = 3
};

ExitStatus evaluate_query(
    const OverallQueryPlan &query_plan,
    const std::function<std::unique_ptr<Writer> (
        const std::shared_ptr<arrow::Schema> &
    )> &writer_factory,
    AliasGenerator &alias_generator
);
