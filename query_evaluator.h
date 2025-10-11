#pragma once

#include <functional>
#include <memory>
#include <optional>

#include <arrow/record_batch.h>
#include <duckdb.hpp>

class Writer;
struct QueryPlan;

struct DuckDbException final : std::runtime_error {
    explicit DuckDbException(const std::string &msg);
};

enum class ExitStatus {
    SUCCESS = 0,
    QUERY_GENERATION_ERROR = 1,
    EXECUTION_ERROR = 2,
    PROGRAMMING_ERROR = 3
};

ExitStatus evaluate_query(
    const std::optional<QueryPlan> &query_plan,
    const std::function<std::unique_ptr<Writer> (const std::shared_ptr<arrow::Schema> &)> &writer_factory
);
