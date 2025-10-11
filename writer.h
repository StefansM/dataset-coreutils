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

inline std::shared_ptr<arrow::io::OutputStream> open_stdout_stream() {
    int const stdout_fd = fileno(stdout);
    if (stdout_fd == -1) {
        throw std::runtime_error("Unable to obtain file number of stdout.");
    }
    return assign_or_raise(arrow::io::FileOutputStream::Open(stdout_fd));
}

inline std::shared_ptr<std::ostream> non_owning_stdout() {
    return std::shared_ptr<std::ostream>(&std::cout, [](std::ostream *) {});
}

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
        const std::shared_ptr<arrow::dataset::FileFormat> &file_format) :
        ArrowDatasetWriter(std::move(schema), file_format, assign_or_raise(arrow::io::FileOutputStream::Open(path))) {}

    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema,
        const std::shared_ptr<arrow::dataset::FileFormat> &file_format) :
        ArrowDatasetWriter(std::move(schema), file_format, open_stdout_stream()) {}


    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema,
        const std::shared_ptr<arrow::dataset::FileFormat> &file_format,
        std::shared_ptr<arrow::io::OutputStream> stream) {

        const auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
        const auto file_options = file_format->DefaultWriteOptions();

        // The "path" parameter in the FileLocator member doesn't seem to be used for anything.
        writer_ = assign_or_raise(file_format->MakeWriter(
            std::move(stream), std::move(schema), file_options, {.filesystem = fs, .path = ""}));
    }

public:
    void write(const std::shared_ptr<arrow::RecordBatch> batch) override {
        if (const auto status = writer_->Write(batch); !status.ok()) {
            throw std::runtime_error("Error writing batch: " + status.ToString());
        }
    }

private:
    std::shared_ptr<arrow::dataset::FileWriter> writer_;
};

class ParquetWriter final : public ArrowDatasetWriter {
    using file_format = arrow::dataset::ParquetFileFormat;

public:
    ParquetWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path) :
        ArrowDatasetWriter(std::move(schema), path, std::make_shared<file_format>()) {}
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
    explicit ColumnarWriter(std::shared_ptr<arrow::Schema> schema) :
        schema_(std::move(schema)),
        stream_(non_owning_stdout()),
        print_options_(print_options()) {

        init();
    }

    ColumnarWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path) :
        schema_(std::move(schema)),
        stream_(open_output_stream(path)),
        print_options_(print_options()) {

        init();
    }


    void write(std::shared_ptr<arrow::RecordBatch> batch) override {
        const auto cols = batch->columns();

        std::int64_t num_rows = 0;
        if (cols.size() == 0) {
            throw std::runtime_error("No columns were provided");
        }
        num_rows = cols[0]->length();

        std::stringstream stream;
        std::vector<std::string> row;

        for (std::int64_t i = 0; i < num_rows; ++i) {
            for (std::size_t j = 0; j < cols.size(); ++j) {
                const auto column = cols[j];
                const auto slice = column->Slice(i, 1);
                if (const auto status = arrow::PrettyPrint(*slice, print_options_, &stream); !status.ok()) {
                    throw std::runtime_error("Error printing column: " + status.ToString());
                }
                auto str = stream.str();
                // Remove comments
                str = std::regex_replace(str, comment_regex_, "");
                str = std::regex_replace(str, newline_regex_, " ");

                row.push_back(str);
                max_col_width_[j] = std::max(max_col_width_[j], str.size());

                stream.str({});
            }
            rendered_rows_.push_back(row);
            row.clear();
        }
    }

    void flush() override {
        for (const auto &row : rendered_rows_) {
            for (std::size_t i = 0; i < row.size(); ++i) {
                const auto width = max_col_width_[i];
                const auto delimiter = i == 0 ? "" : " ";
                (*stream_) << delimiter << std::left << std::setw(width);
                (*stream_) << row[i];
            }
            (*stream_) << '\n';
        }
    }

private:
    static arrow::PrettyPrintOptions print_options() {
        arrow::PrettyPrintOptions print_options{};
        print_options.show_field_metadata = false;
        print_options.array_delimiters = {.open = "", .close = ""};
        print_options.indent_size = 0;
        print_options.skip_new_lines = true;
        return print_options;
    }

    void init() {
        std::vector<std::string> header;
        for (const auto &field: schema_->fields()) {
            const auto name = field->name();
            header.push_back(name);
            max_col_width_.push_back(name.size());
        }
        rendered_rows_.push_back(header);
    }

    static std::shared_ptr<std::ostream> open_output_stream(const std::string &path) {
        const auto stream_ptr = std::make_shared<std::ofstream>(path);
        if (!stream_ptr->is_open()) {
            throw std::runtime_error("Unable to open file " + path + " for writing.");
        }
        return std::static_pointer_cast<std::ostream>(stream_ptr);
    }

    std::regex comment_regex_{"--.*\\n"};
    std::regex newline_regex_{"\\n+"};

    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<std::ostream> stream_;
    arrow::PrettyPrintOptions print_options_;
    std::vector<std::vector<std::string>> rendered_rows_;
    std::vector<std::size_t> max_col_width_;
};

inline std::unique_ptr<Writer> default_writer(const std::shared_ptr<arrow::Schema> &schema) {
    return std::make_unique<CsvWriter>(schema);
}
