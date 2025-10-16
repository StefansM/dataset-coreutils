#include <iostream>
#include <string>

#include <boost/program_options.hpp>
#include <duckdb.hpp>

#include "options.h"
#include "queryplan.h"
#include "query_evaluator.h"
#include "serde.h"
#include "writer.h"


class EvalOptions final : public Options {
public:
    EvalOptions() {
        namespace po = boost::program_options;

        // clang-format off
        description().add_options()
            ("csv,c", po::bool_switch(&write_csv_), "Write results in CSV format.")
            ("parquet,p", po::bool_switch(&write_parquet_), "Write results in Parquet format.")
            ("column,t", po::bool_switch(&write_columnar_), "Write columnated results.")
            ("out,o", po::value(&out_), "Write to this file instead of stdout.")
            ("query,q", po::bool_switch(&print_query_), "Print generated SQL query instead of executing it.");
        // clang-format on
    }

    bool parse(
        const int argc,
        const char *argv[] // NOLINT(*-avoid-c-arrays)
    ) override {
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

    [[nodiscard]] std::unique_ptr<Writer> get_writer(
        const std::shared_ptr<arrow::Schema> &schema
    ) const {
        if (out_.empty()) {
            return stdout_writer(schema);
        }
        return file_writer(schema);
    }

    [[nodiscard]] bool print_query() const {
        return print_query_;
    }

private:
    [[nodiscard]] std::unique_ptr<Writer> stdout_writer(
        const std::shared_ptr<arrow::Schema> &schema
    ) const {
        if (write_csv_) {
            return std::make_unique<CsvWriter>(schema);
        }
        if (write_parquet_) {
            throw std::runtime_error("Parquet output requires a seekable stream; cannot write to stdout.");
        }
        if (write_columnar_) {
            return std::make_unique<ColumnarWriter>(schema);
        }

        throw std::logic_error("Invariant failure: Neither write_csv or write_parquet set.");
    }

    [[nodiscard]] std::unique_ptr<Writer> file_writer(
        const std::shared_ptr<arrow::Schema> &schema
    ) const {
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
    bool print_query_{false};
};


int main(
    const int argc,
    const char *argv[]
) {
    EvalOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    const auto overall_query_plan = load_query_plan(std::cin);
    if (!overall_query_plan) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return 1;
    }

    const auto writer_factory = [&options](
        const std::shared_ptr<arrow::Schema> &schema
    ) {
        return options.get_writer(schema);
    };

    AliasGenerator alias_generator;
    if (options.print_query()) {
        auto query = overall_query_plan->generate_query(alias_generator);
        if (!query) {
            std::cerr << "Error generating query from query plan.\n";
            return static_cast<int>(ExitStatus::QUERY_GENERATION_ERROR);
        }
        const auto [query_str, query_params] = *query;
        std::cout << query_str << '\n';
        for (const auto &[column, value]: query_params) {
            std::cout << "-- Colum " << column << ": " << value << '\n';
        }
        return static_cast<int>(ExitStatus::SUCCESS);
    }

    return static_cast<int>(evaluate_query(*overall_query_plan, writer_factory, alias_generator));
}
