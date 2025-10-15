#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>
#include <type_traits>
#include <stdexcept>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <duckdb.h>

#include "arrow_result.h"
#include "options.h"
#include "query.h"
#include "queryplan.h"
#include "writer.h"

#include "query_evaluator.h"


DuckDbException::DuckDbException(
    const std::string &msg
) :
    std::runtime_error(msg) {}

namespace {
struct DatabaseCloser {
    void operator()(
        duckdb_database db
    ) const {
        if (db != nullptr) {
            duckdb_close(&db);
        }
    }
};

struct ConnectionCloser {
    void operator()(
        duckdb_connection connection
    ) const {
        if (connection != nullptr) {
            duckdb_disconnect(&connection);
        }
    }
};

struct PreparedStatementDestroyer {
    void operator()(
        duckdb_prepared_statement statement
    ) const {
        if (statement != nullptr) {
            duckdb_destroy_prepare(&statement);
        }
    }
};

struct DataChunkDestroyer {
    void operator()(
        duckdb_data_chunk chunk
    ) const {
        if (chunk != nullptr) {
            duckdb_destroy_data_chunk(&chunk);
        }
    }
};

struct ArrowOptionsDestroyer {
    void operator()(
        duckdb_arrow_options options
    ) const {
        if (options != nullptr) {
            duckdb_destroy_arrow_options(&options);
        }
    }
};

using UniqueDatabase = std::unique_ptr<std::remove_pointer_t<duckdb_database>, DatabaseCloser>;
using UniqueConnection = std::unique_ptr<std::remove_pointer_t<duckdb_connection>, ConnectionCloser>;
using UniquePreparedStatement = std::unique_ptr<std::remove_pointer_t<duckdb_prepared_statement>,
    PreparedStatementDestroyer>;
using UniqueDataChunk = std::unique_ptr<std::remove_pointer_t<duckdb_data_chunk>, DataChunkDestroyer>;
using UniqueArrowOptions = std::unique_ptr<std::remove_pointer_t<duckdb_arrow_options>, ArrowOptionsDestroyer>;

struct ResultGuard {
    duckdb_result *result;

    ResultGuard(const ResultGuard &) = delete;
    ResultGuard(ResultGuard &&) = delete;
    ResultGuard &operator=(const ResultGuard &) = delete;
    ResultGuard &operator=(ResultGuard &&) = delete;

    ~ResultGuard() {
        if (result != nullptr) {
            duckdb_destroy_result(result);
        }
    }
};

[[noreturn]] void throw_duckdb_error(
    const std::string &context,
    const char *error_message
) {
    if (error_message != nullptr) {
        throw DuckDbException(context + " " + error_message);
    }
    throw DuckDbException(context);
}

template<typename Binder>
duckdb_state bind_checked(
    const Binder &binder,
    const duckdb_prepared_statement statement,
    const std::string &context
) {
    const auto state = binder();
    if (state == DuckDBError) {
        throw_duckdb_error(context, duckdb_prepare_error(statement));
    }
    return state;
}
} // namespace

static void bind_inferred_value(
    const duckdb_prepared_statement statement,
    const idx_t param_index,
    const ColumnQueryParam &param,
    const std::unordered_map<std::string, std::string> &param_types
) {
    using std::string_literals::operator ""s;

    if (const auto col_entry = param_types.find(param.column); col_entry != param_types.end()) {
        const auto col_type = col_entry->second;
        const auto string_value = param.value.get<std::string>();

        if (col_type == "BIGINT" || col_type == "INTEGER" || col_type == "SMALLINT") {
            try {
                const auto integer_value = std::stoll(string_value);
                bind_checked(
                    [&] {
                        return duckdb_bind_int64(statement, param_index, integer_value);
                    },
                    statement,
                    "Could not bind inferred integer parameter."s
                );
                return;
            } catch (const std::exception &e) {
                throw std::runtime_error(
                    "Could not convert parameter "s + string_value + " to integer for column '" + param.column + "': " +
                    e.what()
                );
            }
        }
        if (col_type == "DOUBLE" || col_type == "FLOAT") {
            try {
                const auto double_value = std::stod(string_value);
                bind_checked(
                    [&] {
                        return duckdb_bind_double(statement, param_index, double_value);
                    },
                    statement,
                    "Could not bind inferred double parameter."s
                );
                return;
            } catch (const std::exception &e) {
                throw std::runtime_error(
                    "Could not convert parameter "s + string_value + " to float for column '" + param.column + "': " + e
                    .what()
                );
            }
        }
        if (col_type == "VARCHAR" || col_type == "TEXT") {
            const auto &string_param_value = param.value.get<std::string>();
            bind_checked(
                [&] {
                    return duckdb_bind_varchar(statement, param_index, string_param_value.c_str());
                },
                statement,
                "Could not bind inferred text parameter."s
            );
            return;
        }
        throw DuckDbException("Unable to infer type for param for column '" + param.column + "'.");
    }
    throw std::runtime_error("Could not find column '" + param.column + "' in schema.");
}

static void bind_params_to_statement(
    const duckdb_prepared_statement statement,
    const std::vector<ColumnQueryParam> &query_params,
    const std::unordered_map<std::string, std::string> &param_types
) {
    using std::string_literals::operator ""s;

    idx_t param_index = 1;
    for (const auto &param: query_params) {
        switch (param.value.type()) {
            case ParamType::NUMERIC:
                bind_checked(
                    [&] {
                        return duckdb_bind_int64(statement, param_index, param.value.get<std::int64_t>());
                    },
                    statement,
                    "Could not bind numeric parameter."s
                );
                break;

            case ParamType::TEXT: {
                const auto &string_value = param.value.get<std::string>();
                bind_checked(
                    [&] {
                        return duckdb_bind_varchar(statement, param_index, string_value.c_str());
                    },
                    statement,
                    "Could not bind text parameter."s
                );
                break;
            }

            case ParamType::UNKNOWN:
                bind_inferred_value(statement, param_index, param, param_types);
                break;

            default:
                throw DuckDbException(
                    "Unable to convert param " + std::to_string(param_index - 1) + " to DuckDb type."
                );
        }
        ++param_index;
    }
}

static auto duckdb_schema_to_arrow(
    const duckdb_connection connection,
    duckdb_prepared_statement statement
) -> std::shared_ptr<arrow::Schema> {
    duckdb_arrow_options raw_arrow_options = nullptr;
    duckdb_connection_get_arrow_options(connection, &raw_arrow_options);
    if (raw_arrow_options == nullptr) {
        throw DuckDbException("Could not fetch DuckDB Arrow options.");
    }
    const UniqueArrowOptions arrow_options(raw_arrow_options);

    const auto column_count = duckdb_prepared_statement_column_count(statement);
    std::vector<duckdb_logical_type> logical_types;
    logical_types.reserve(column_count);
    std::vector<const char *> column_names;
    column_names.reserve(column_count);

    for (idx_t idx = 0; idx < column_count; ++idx) {
        logical_types.push_back(duckdb_prepared_statement_column_logical_type(statement, idx));
        column_names.push_back(duckdb_prepared_statement_column_name(statement, idx));
    }

    ArrowSchema arrow_schema{};
    duckdb_error_data error_data = duckdb_to_arrow_schema(
        arrow_options.get(),
        logical_types.data(),
        column_names.data(),
        column_count,
        &arrow_schema
    );

    for (auto &logical_type: logical_types) {
        duckdb_destroy_logical_type(&logical_type);
    }

    if (duckdb_error_data_has_error(error_data)) {
        const auto *message = duckdb_error_data_message(error_data);
        duckdb_destroy_error_data(&error_data);
        throw_duckdb_error("Could not convert schema to Arrow.", message);
    }
    duckdb_destroy_error_data(&error_data);

    return assign_or_raise(arrow::ImportSchema(&arrow_schema));
}


auto chunk_to_record_batch(
    const duckdb_arrow_options arrow_options,
    duckdb_data_chunk chunk,
    std::shared_ptr<arrow::Schema> arrow_schema
) -> std::shared_ptr<arrow::RecordBatch> {
    ArrowArray arrow_array{};
    duckdb_error_data error_data = duckdb_data_chunk_to_arrow(arrow_options, chunk, &arrow_array);
    if (duckdb_error_data_has_error(error_data)) {
        const auto *message = duckdb_error_data_message(error_data);
        duckdb_destroy_error_data(&error_data);
        throw_duckdb_error("Could not convert chunk to Arrow.", message);
    }
    duckdb_destroy_error_data(&error_data);

    return assign_or_raise(arrow::ImportRecordBatch(&arrow_array, std::move(arrow_schema)));
}

static std::unordered_map<std::string, std::string> get_schema(
    const QueryPlan &query_plan,
    duckdb_connection connection
) {
    using namespace std::string_literals;

    QueryPlan base_query = query_plan;
    base_query.limit = std::nullopt;
    base_query.order = std::nullopt;
    base_query.where = std::nullopt;

    AliasGenerator alias_generator;
    const auto query = base_query.generate_query(alias_generator);
    if (!query) {
        throw std::runtime_error("Error generating query from query plan.\n");
    }
    const auto [base_query_str, query_params] = *query;
    if (!query_params.empty()) {
        throw std::logic_error("Stripping limit, order and where clauses should result in no query parameters.");
    }

    const auto describe_query = "DESCRIBE ("s + base_query_str + ")"s;

    std::unordered_map<std::string, std::string> column_types;

    duckdb_result result{};
    const ResultGuard result_guard{&result};
    if (duckdb_query(connection, describe_query.c_str(), &result) == DuckDBError) {
        const auto *error_message = duckdb_result_error(&result);
        throw_duckdb_error("Error executing DESCRIBE query.", error_message);
    }

    const auto row_count = duckdb_row_count(&result);
    for (idx_t row = 0; row < row_count; ++row) {
        const std::unique_ptr<char, decltype(&duckdb_free)> column_name(
            duckdb_value_varchar(&result, 0, row),
            duckdb_free
        );
        const std::unique_ptr<char, decltype(&duckdb_free)> column_type(
            duckdb_value_varchar(&result, 1, row),
            duckdb_free
        );

        if (!column_name || !column_type) {
            throw DuckDbException("Failed to read schema information from DuckDB.");
        }
        column_types[column_name.get()] = column_type.get();
    }

    return column_types;
}

ExitStatus evaluate_query(
    const QueryPlan &query_plan,
    const std::function<std::unique_ptr<Writer> (
        const std::shared_ptr<arrow::Schema> &
    )> &writer_factory,
    AliasGenerator &alias_generator
) {
    auto query = query_plan.generate_query(alias_generator);
    if (!query) {
        std::cerr << "Error generating query from query plan.\n";
        return ExitStatus::QUERY_GENERATION_ERROR;
    }

    duckdb_database raw_db = nullptr;
    if (duckdb_open(nullptr, &raw_db) == DuckDBError) {
        throw DuckDbException("Failed to open DuckDB in-memory database.");
    }
    const UniqueDatabase database(raw_db);

    duckdb_connection raw_connection = nullptr;
    if (duckdb_connect(database.get(), &raw_connection) == DuckDBError) {
        throw DuckDbException("Failed to connect to DuckDB database.");
    }
    const UniqueConnection connection(raw_connection);
    auto [query_str, query_params] = *query;

    try {
        const auto param_types = get_schema(query_plan, connection.get());

        duckdb_prepared_statement raw_statement = nullptr;
        const auto prepare_state = duckdb_prepare(connection.get(), query_str.c_str(), &raw_statement);
        const UniquePreparedStatement prepared_statement(raw_statement);
        if (prepare_state == DuckDBError) {
            const auto *error_message = prepared_statement ? duckdb_prepare_error(prepared_statement.get()) : nullptr;
            throw_duckdb_error("Failed to prepare statement.", error_message);
        }

        bind_params_to_statement(prepared_statement.get(), query_params, param_types);

        duckdb_result result{};
        const ResultGuard result_guard{&result};
        if (duckdb_execute_prepared(prepared_statement.get(), &result) == DuckDBError) {
            const auto *error_message = duckdb_result_error(&result);
            throw_duckdb_error("Error executing prepared statement.", error_message);
        }

        duckdb_arrow_options raw_arrow_options = duckdb_result_get_arrow_options(&result);
        if (raw_arrow_options == nullptr) {
            throw DuckDbException("DuckDB did not provide Arrow options for the result.");
        }
        const UniqueArrowOptions arrow_options(raw_arrow_options);

        const auto arrow_schema = duckdb_schema_to_arrow(connection.get(), prepared_statement.get());
        const auto writer = writer_factory(arrow_schema);

        const auto chunk_count = duckdb_result_chunk_count(result);
        for (idx_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
            duckdb_data_chunk raw_chunk = duckdb_result_get_chunk(result, chunk_index);
            const UniqueDataChunk chunk(raw_chunk);
            if (!chunk || duckdb_data_chunk_get_size(chunk.get()) == 0) {
                continue;
            }

            const auto batch_result = chunk_to_record_batch(arrow_options.get(), chunk.get(), arrow_schema);
            writer->write(batch_result);
        }
        writer->flush();
    } catch (const std::runtime_error &error) {
        std::cerr << "Error executing statement or writing results. " << error.what() << '\n';
        return ExitStatus::EXECUTION_ERROR;
    } catch (const std::logic_error &error) {
        std::cerr << "Programming error executing statement or writing results. " << error.what() << '\n';
        return ExitStatus::PROGRAMMING_ERROR;
    }
    return ExitStatus::SUCCESS;
}
