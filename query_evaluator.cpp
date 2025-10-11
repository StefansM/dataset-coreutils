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

DuckDbException::DuckDbException(const std::string &msg) : std::runtime_error(msg) {}

template<typename T>
T dd_check(T result) {
    if (result->HasError()) {
        throw DuckDbException("Error doing DuckDb action. " + result->GetErrorObject().Message());
    }
    return result;
}

static duckdb::vector<duckdb::Value> convert_params_to_duckdb(const std::vector<QueryParam> &query_params) {
    duckdb::vector<duckdb::Value> duckdb_params;

    int i = 0;
    for (const auto &p: query_params) {
        duckdb::Value value;
        switch (p.type()) {
            case ParamType::NUMERIC:
                value = duckdb::Value::CreateValue<std::int64_t>(p.get<std::int64_t>());
                break;

            case ParamType::TEXT:
            case ParamType::UNKNOWN:
                value = duckdb::Value::CreateValue<std::string>(p.get<std::string>());
                break;

            default:
                throw DuckDbException("Unable to convert param " + std::to_string(i) + " to DuckDb type.");
        }
        duckdb_params.push_back(value);
        ++i;
    }

    return duckdb_params;
}

static std::shared_ptr<arrow::Schema> duckdb_schema_to_arrow(const std::unique_ptr<duckdb::QueryResult> &result) {
    ArrowSchema duck_arrow_schema{};
    duckdb::ArrowConverter::ToArrowSchema(&duck_arrow_schema, result->types, result->names, result->client_properties);

    return assign_or_raise(arrow::ImportSchema(&duck_arrow_schema));
}


std::shared_ptr<arrow::RecordBatch> chunk_to_record_batch(const std::unique_ptr<duckdb::DataChunk> &data_chunk,
                                                          std::shared_ptr<arrow::Schema> arrow_schema,
                                                          const std::unique_ptr<duckdb::QueryResult> &result) {
    ArrowArray arrow_array{};

    // TODO: No need to recreate this multiple times
    const std::unordered_map<duckdb::idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>>
            extension_type_cast;
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &arrow_array, result->client_properties, extension_type_cast);

    return assign_or_raise(arrow::ImportRecordBatch(&arrow_array, std::move(arrow_schema)));
}

ExitStatus evaluate_query(
    const std::optional<QueryPlan> &query_plan,
    const std::function<std::unique_ptr<Writer> (const std::shared_ptr<arrow::Schema> &)> &writer_factory
) {
    auto query = query_plan->generate_query();
    if (!query) {
        std::cerr << "Error generating query from query plan.\n";
        return ExitStatus::QUERY_GENERATION_ERROR;
    }

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto [query_str, query_params] = *query;

    try {
        const auto prepared_statement = dd_check(con.Prepare(query_str));
        auto duckdb_params = convert_params_to_duckdb(query_params);
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
    return  ExitStatus::SUCCESS;
}
