#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <duckdb.hpp>

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

template<typename T>
T dd_check(
    T result
) {
    if (result->HasError()) {
        throw DuckDbException("Error doing DuckDb action. " + result->GetErrorObject().Message());
    }
    return result;
}

static duckdb::Value infer_value_from_schema(
    const ColumnQueryParam &param,
    const std::unordered_map<std::string, std::string> &param_types
) {
    using std::string_literals::operator ""s;

    if (const auto col_entry = param_types.find(param.column); col_entry != param_types.end()) {
        const auto col_type = col_entry->second;
        const auto string_value = param.value.get<std::string>();

        if (col_type == "BIGINT" || col_type == "INTEGER" || col_type == "SMALLINT") {
            try {
                return duckdb::Value::CreateValue<std::int64_t>(std::stoll(string_value));
            } catch (const std::exception &e) {
                throw std::runtime_error(
                    "Could not convert paramter "s + string_value + " to integer for column '" + param.column + "': " +
                    e.what()
                );
            }
        }
        if (col_type == "DOUBLE" || col_type == "FLOAT") {
            try {
                return duckdb::Value::CreateValue<double>(std::stod(string_value));
            } catch (const std::exception &e) {
                throw std::runtime_error(
                    "Could not convert paramter "s + string_value + " to float for column '" + param.column + "': " + e.
                    what()
                );
            }
        }
        if (col_type == "VARCHAR" || col_type == "TEXT") {
            return duckdb::Value::CreateValue<std::string>(param.value.get<std::string>());
        }
        throw DuckDbException("Unable to infer type for param for column '" + param.column + "'.");
    }
    throw std::runtime_error("Could not find column '" + param.column + "' in schema.");
}

static duckdb::vector<duckdb::Value> convert_params_to_duckdb(
    const std::vector<ColumnQueryParam> &query_params,
    const std::unordered_map<std::string, std::string> &param_types
) {
    duckdb::vector<duckdb::Value> duckdb_params;

    int i = 0;
    for (const auto &param: query_params) {
        duckdb::Value value;
        switch (param.value.type()) {
            case ParamType::NUMERIC:
                value = duckdb::Value::CreateValue<std::int64_t>(param.value.get<std::int64_t>());
                break;

            case ParamType::TEXT:
                value = duckdb::Value::CreateValue<std::string>(param.value.get<std::string>());
                break;

            case ParamType::UNKNOWN:
                value = infer_value_from_schema(param, param_types);
                break;

            default:
                throw DuckDbException("Unable to convert param " + std::to_string(i) + " to DuckDb type.");
        }
        duckdb_params.push_back(value);
        ++i;
    }

    return duckdb_params;
}

static std::shared_ptr<arrow::Schema> duckdb_schema_to_arrow(
    const std::unique_ptr<duckdb::QueryResult> &result
) {
    ArrowSchema duck_arrow_schema{};
    duckdb::ArrowConverter::ToArrowSchema(&duck_arrow_schema, result->types, result->names, result->client_properties);

    return assign_or_raise(arrow::ImportSchema(&duck_arrow_schema));
}


std::shared_ptr<arrow::RecordBatch> chunk_to_record_batch(
    const std::unique_ptr<duckdb::DataChunk> &data_chunk,
    std::shared_ptr<arrow::Schema> arrow_schema,
    const std::unique_ptr<duckdb::QueryResult> &result
) {
    ArrowArray arrow_array{};

    // TODO: No need to recreate this multiple times
    const std::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>>
            extension_type_cast;
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &arrow_array, result->client_properties, extension_type_cast);

    return assign_or_raise(arrow::ImportRecordBatch(&arrow_array, std::move(arrow_schema)));
}

static std::unordered_map<std::string, std::string> get_schema(
    const QueryPlan &query_plan,
    duckdb::Connection &conn
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

    const auto prepared_statement = dd_check(conn.Prepare(describe_query));
    const auto result = dd_check(prepared_statement->Execute());

    for (auto data_chunk = result->Fetch(); data_chunk && data_chunk->size() > 0; data_chunk = result->Fetch()) {
        for (duckdb::idx_t row = 0; row < data_chunk->size(); ++row) {
            const auto column_name = data_chunk->GetValue(0, row).ToString();
            const auto column_type = data_chunk->GetValue(1, row).ToString();
            column_types[column_name] = column_type;
        }
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

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto [query_str, query_params] = *query;

    try {
        const auto param_types = get_schema(query_plan, con);

        const auto prepared_statement = dd_check(con.Prepare(query_str));
        auto duckdb_params = convert_params_to_duckdb(query_params, param_types);
        const auto result = dd_check(prepared_statement->Execute(duckdb_params, true));
        const auto arrow_schema = duckdb_schema_to_arrow(result);
        const auto writer = writer_factory(arrow_schema);

        while (true) {
            auto data_chunk = result->Fetch();
            if (!data_chunk || data_chunk->size() == 0) {
                break;
            }

            const auto batch_result = chunk_to_record_batch(data_chunk, arrow_schema, result);
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
