#include <cstdio>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <boost/program_options.hpp>
#include <duckdb.hpp>

#include "arrow_result.h"
#include "options.h"
#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "writer.h"


struct DuckDbException final : std::runtime_error {
    explicit DuckDbException(const std::string &msg) : std::runtime_error(msg) {}
};

class EvalOptions final : public Options {
public:
    EvalOptions() {
        namespace po = boost::program_options;

        description().add_options()
            ("csv,c", po::bool_switch(&write_csv_), "Write results in CSV format.")
            ("parquet,p", po::bool_switch(&write_parquet_), "Write results in Parquet format.")
            ("column,t", po::bool_switch(&write_columnar_), "Write columnated results.")
            ("out,o", po::value(&out_), "Write to this file instead of stdout.");
    }

    bool parse(const int argc, const char *argv[]) override { // NOLINT(*-avoid-c-arrays)
        if (bool const parent_result = Options::parse(argc, argv); !parent_result) {
            return parent_result;
        }

        int num_formats = 0;
        num_formats += write_csv_ ? 1 : 0;
        num_formats += write_parquet_ ? 1 : 0;
        num_formats += write_columnar_ ? 1 : 0;

        if (num_formats > 1) {
            std::cerr << "Only one of 'csv', 'parquet' or 'column' may be specified.\n";
            return false;
        }

        // Default output format is CSV.
        if (num_formats == 0) {
            write_csv_ = true;
        }

        return true;
    }

    [[nodiscard]] std::unique_ptr<Writer> get_writer(const std::shared_ptr<arrow::Schema> &schema) const {
        if (out_.empty()) {
            return stdout_writer(schema);
        }
        return file_writer(schema);
    }

private:
    [[nodiscard]] std::unique_ptr<Writer> stdout_writer(const std::shared_ptr<arrow::Schema> &schema) const {
        int const stdout_fd = fileno(stdout);
        if (stdout_fd == -1) {
            throw std::runtime_error("Unable to obtain file number of stdout.");
        }

        if (write_csv_) {
            return std::make_unique<CsvWriter>(schema, stdout_fd);
        }
        if (write_parquet_) {
            throw std::runtime_error("Parquet output requires a seekable stream; cannot write to stdout.");
        }
        if (write_columnar_) {
            return std::make_unique<ColumnarWriter>(schema);
        }

        throw std::logic_error("Invariant failure: Neither write_csv or write_parquet set.");
    }

    [[nodiscard]] std::unique_ptr<Writer> file_writer(const std::shared_ptr<arrow::Schema> &schema) const {
        if (write_csv_) {
            return std::make_unique<CsvWriter>(schema, out_);
        }
        if (write_parquet_) {
            return std::make_unique<ParquetWriter>(schema, out_);
        }
        if (write_columnar_) {
            return std::make_unique<ColumnarWriter>(schema, out_);
        }

        throw std::logic_error("Invariant failure: Neither write_csv or write_parquet set.");
    }


    bool write_csv_{};
    bool write_parquet_{};
    bool write_columnar_{};
    std::string out_;
};

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

int main(const int argc, const char *argv[]) {
    EvalOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    const auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return 1;
    }

    auto query = query_plan->generate_query();
    if (!query) {
        return 1;
    }

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto [query_str, query_params] = *query;

    // std::cerr << "Generated query: \n" << query_str << "\n\n";
    try {
        const auto prepared_statement = dd_check(con.Prepare(query_str));
        auto duckdb_params = convert_params_to_duckdb(query_params);
        const auto result = dd_check(prepared_statement->Execute(duckdb_params, true));
        const auto arrow_schema = duckdb_schema_to_arrow(result);
        const auto writer = options.get_writer(arrow_schema);

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
        return 2;
    } catch (const std::logic_error &error) {
        std::cerr << "Programming error executing statement or writing results. " << error.what() << '\n';
        return 3;
    }

    return 0;
}
