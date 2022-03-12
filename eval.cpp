#include <iostream>
#include <string>
#include <vector>
#include <cstdint>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <duckdb.hpp>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"


int main(int argc, char **argv) {
    Options options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    auto query = query_plan->generate_query();
    if (!query) {
        return 1;
    }

    auto [query_str, query_params] = *query;

    std::cout << "Generated query: " << std::endl << query_str << std::endl << std::endl;

    auto prepared_statement = con.Prepare(query_str);
    if (!prepared_statement->success) {
        std::cerr << "Error constructing statement: " << prepared_statement->error << std::endl;
        return 2;
    }

    std::vector<duckdb::Value> duckdb_params;
    for (const auto &p : query_params) {
        switch (p.type) {
            case ParamType::NUMERIC:
                duckdb_params.push_back(duckdb::Value::CreateValue<std::int64_t>(p.get<std::int64_t>()));
                break;

            case ParamType::TEXT:
            case ParamType::UNKNOWN:
                duckdb_params.push_back(duckdb::Value::CreateValue<std::string>(p.get<std::string>()));
                break;
        }
    }

    auto result = prepared_statement->Execute(duckdb_params, true);
    if (!result->success) {
        std::cerr << "Error querying DuckDB: " << result->error << std::endl;
        return 2;
    }

    result->Print();

    ArrowSchema duck_arrow_schema;
    result->ToArrowSchema(&duck_arrow_schema);

    auto arrow_schema_result = arrow::ImportSchema(&duck_arrow_schema);
    if (!arrow_schema_result.ok()) {
        std::cerr << "Error retrieving Arrow schema from DuckDb result: " << arrow_schema_result.status().ToString() << std::endl;
        return 2;
    }

    auto arrow_schema = *arrow_schema_result;
    auto filesystem = std::make_shared<arrow::fs::LocalFileSystem>();
    auto file_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    auto output_stream = arrow::io::FileOutputStream::Open("test.parquet");
    if (!output_stream.ok()) {
        std::cerr << "Error constructing output stream: " << output_stream.status().ToString() << std::endl;
        return 2;
    }

    auto writer_options = file_format->DefaultWriteOptions();
    auto writer_result = file_format->MakeWriter(
            *output_stream,
            arrow_schema,
            writer_options,
            {filesystem, "foo"});

    if (!writer_result.ok()) {
        std::cerr << "Error constructing writer: " << writer_result.status().ToString() << std::endl;
        return 2;
    }

    auto writer = *writer_result;
    while (true) {
        auto data_chunk = result->Fetch();
        if (!data_chunk || data_chunk->size() == 0) {
            break;
        }

        ArrowArray arrow_array;
        data_chunk->ToArrowArray(&arrow_array);

        auto batch_result = arrow::ImportRecordBatch(&arrow_array, arrow_schema);
        if (!batch_result.ok()) {
            std::cerr << "Error retrieving batch: " << batch_result.status().ToString() << std::endl;
            break;
        }

        auto write_result = writer->Write(*batch_result);
    }

    return 0;
}
