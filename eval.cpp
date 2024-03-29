#include <exception>
#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include <cstdio>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <boost/program_options.hpp>
#include <duckdb.hpp>

#include "arrow_result.h"
#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"
#include "writer.h"


struct DuckDbException : public std::runtime_error {
    DuckDbException(std::string msg) : std::runtime_error(msg) {}
};

class EvalOptions : public Options {
public:
    EvalOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("csv,c", po::bool_switch(&write_csv_), "Write results in CSV format.")
            ("parquet,p", po::bool_switch(&write_parquet_), "Write results in Parquet format.")
            ("out,o", po::value(&out_), "Write to this file instead of stdout.")
        ;
    }

    virtual bool parse(int argc, char **argv) override {
        bool parent_result = Options::parse(argc, argv);
        if (!parent_result) {
            return parent_result;
        }

        if (write_csv_ && write_parquet_) {
            std::cerr << "Only one of 'integer' or 'text' may be specified." << std::endl;
            return false;
        }

        // Default output format is CSV.
        if (!write_csv_ && !write_parquet_) {
            write_csv_ = true;
        }

        return true;
    }

    std::unique_ptr<Writer> get_writer(std::shared_ptr<arrow::Schema> schema) const {
        if (out_.empty())
            return stdout_writer(schema);
        else
            return file_writer(schema);
    }

private:
    std::unique_ptr<Writer> stdout_writer(std::shared_ptr<arrow::Schema> schema) const {
        int stdout_fd = -1;
        stdout_fd = ::fileno(stdout);
        if (stdout_fd == -1)
            throw std::runtime_error("Unable to obtain fileno of stdout.");

        if (write_csv_)
            return std::make_unique<CsvWriter>(schema, stdout_fd);
        else if (write_parquet_)
            throw std::runtime_error("Parquet output requires a seekable stream; cannot write to stdout.");

        throw std::logic_error("Invariant failure: Neither write_csv or write_parquet set.");
    }

    std::unique_ptr<Writer> file_writer(std::shared_ptr<arrow::Schema> schema) const {
        if (write_csv_)
            return std::make_unique<CsvWriter>(schema, out_);
        else if (write_parquet_)
            return std::make_unique<ParquetWriter>(schema, out_);

        throw std::logic_error("Invariant failure: Neither write_csv or write_parquet set.");
    }

private:
    bool write_csv_;
    bool write_parquet_;
    std::string out_;
};

template <typename T>
T dd_check(T result) {
    if (!result->success) {
        throw DuckDbException("Error doing DuckDb action. " + result->error);
    }
    return result;
}

static std::vector<duckdb::Value> convert_params_to_duckdb(const std::vector<QueryParam> &query_params) {
    std::vector<duckdb::Value> duckdb_params;

    int i = 0;
    for (const auto &p : query_params) {
        duckdb::Value value;
        switch (p.type) {
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

static std::shared_ptr<arrow::Schema> duckdb_schema_to_arrow(std::unique_ptr<duckdb::QueryResult> &result) {
    ArrowSchema duck_arrow_schema;
    result->ToArrowSchema(&duck_arrow_schema);

    return assign_or_raise(arrow::ImportSchema(&duck_arrow_schema));
}


std::shared_ptr<arrow::RecordBatch> chunk_to_record_batch(
        std::unique_ptr<duckdb::DataChunk> &data_chunk,
        std::shared_ptr<arrow::Schema> arrow_schema) {
    ArrowArray arrow_array;
    data_chunk->ToArrowArray(&arrow_array);

    return assign_or_raise(arrow::ImportRecordBatch(&arrow_array, arrow_schema));
}

int main(int argc, char **argv) {
    EvalOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    auto query = query_plan->generate_query();
    if (!query) {
        return 1;
    }

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto [query_str, query_params] = *query;

    std::cerr << "Generated query: " << std::endl << query_str << std::endl << std::endl;
    try {
        auto prepared_statement = dd_check(con.Prepare(query_str));
        auto duckdb_params = convert_params_to_duckdb(query_params);
        auto result = dd_check(prepared_statement->Execute(duckdb_params, true));
        auto arrow_schema = duckdb_schema_to_arrow(result);
        auto writer = options.get_writer(arrow_schema);

        while (true) {
            auto data_chunk = result->Fetch();
            if (!data_chunk || data_chunk->size() == 0) {
                break;
            }

            auto batch_result = chunk_to_record_batch(data_chunk, arrow_schema);
            writer->write(batch_result);
        }
    } catch (const std::runtime_error &error) {
        std::cerr << "Error executing statement or writing results. " << error.what() << std::endl;
        return 2;
    }

    return 0;
}
