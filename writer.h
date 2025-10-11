#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <regex>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>

#include "arrow_result.h"

class Writer;

std::shared_ptr<arrow::io::OutputStream> open_stdout_stream();
std::shared_ptr<std::ostream> non_owning_stdout();
std::unique_ptr<Writer> default_writer(const std::shared_ptr<arrow::Schema> &schema);

class Writer {
public:
    virtual void write(std::shared_ptr<arrow::RecordBatch> batch) = 0;
    virtual void flush() {}

    virtual ~Writer() = default;
    Writer() = default;

    Writer(const Writer &) = delete;
    Writer &operator=(const Writer &) = delete;
    Writer(Writer &&) = delete;
    Writer &operator=(Writer &&) = delete;
};

class ArrowDatasetWriter : public Writer {
protected:
    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path,
        const std::shared_ptr<arrow::dataset::FileFormat> &file_format);

    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema,
        const std::shared_ptr<arrow::dataset::FileFormat> &file_format);


    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema,
        const std::shared_ptr<arrow::dataset::FileFormat> &file_format,
        std::shared_ptr<arrow::io::OutputStream> stream);

public:
    void write(std::shared_ptr<arrow::RecordBatch> batch) override;

private:
    std::shared_ptr<arrow::dataset::FileWriter> writer_;
};

class ParquetWriter final : public ArrowDatasetWriter {
    using file_format = arrow::dataset::ParquetFileFormat;

public:
    ParquetWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path);
};

class CsvWriter final : public ArrowDatasetWriter {
    using file_format = arrow::dataset::CsvFileFormat;

public:
    CsvWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path) :
        ArrowDatasetWriter(std::move(schema), path, std::make_shared<file_format>()) {}

    explicit CsvWriter(std::shared_ptr<arrow::Schema> schema) :
        ArrowDatasetWriter(std::move(schema), std::make_shared<file_format>()) {}
};

class ColumnarWriter final : public Writer {
public:
    explicit ColumnarWriter(std::shared_ptr<arrow::Schema> schema);

    ColumnarWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path);


    void write(std::shared_ptr<arrow::RecordBatch> batch) override;

    void flush() override;

private:
    static arrow::PrettyPrintOptions print_options();

    void init();

    static std::shared_ptr<std::ostream> open_output_stream(const std::string &path);

    std::regex comment_regex_{"--.*\\n"};
    std::regex newline_regex_{"\\n+"};

    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<std::ostream> stream_;
    arrow::PrettyPrintOptions print_options_;
    std::vector<std::vector<std::string>> rendered_rows_;
    std::vector<std::size_t> max_col_width_;
};

