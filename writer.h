#pragma once

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>

#include "arrow_result.h"

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

    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema, const int fd,
                       const std::shared_ptr<arrow::dataset::FileFormat> &file_format) :
        ArrowDatasetWriter(std::move(schema), file_format, assign_or_raise(arrow::io::FileOutputStream::Open(fd))) {}


    ArrowDatasetWriter(std::shared_ptr<arrow::Schema> schema,
                       const std::shared_ptr<arrow::dataset::FileFormat> &file_format,
                       std::shared_ptr<arrow::io::OutputStream> stream) {

        const auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
        const auto file_options = file_format->DefaultWriteOptions();

        // The "path" parameter in the FileLocator member doesn't seem to be used for anything.
        writer_ = assign_or_raise(file_format->MakeWriter(std::move(stream), std::move(schema), file_options,
                                                          {.filesystem = fs, .path = ""}));
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

    ParquetWriter(std::shared_ptr<arrow::Schema> schema, const int fd) :
        ArrowDatasetWriter(std::move(schema), fd, std::make_shared<file_format>()) {}
};

class CsvWriter final : public ArrowDatasetWriter {
    using file_format = arrow::dataset::CsvFileFormat;

public:
    CsvWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path) :
        ArrowDatasetWriter(std::move(schema), path, std::make_shared<file_format>()) {}

    CsvWriter(std::shared_ptr<arrow::Schema> schema, const int fd) :
        ArrowDatasetWriter(std::move(schema), fd, std::make_shared<file_format>()) {}
};

class ColumnarWriter final : public Writer {
public:
    explicit ColumnarWriter(std::shared_ptr<arrow::Schema> schema) :
        schema_(std::move(schema)), stream_(std::shared_ptr<std::ostream>(&std::cout, [](std::ostream *) {})) {}

    ColumnarWriter(std::shared_ptr<arrow::Schema> schema, const std::string &path) :
        schema_(std::move(schema)), stream_(open_output_stream(path)) {}


    void write(std::shared_ptr<arrow::RecordBatch> batch) override {
        // Accumulate batches, and only print when flushing the writer.
        if (batch->num_rows() == 0) {
            return;
        }
        batches_.push_back(batch);
    }

    void flush() override {
        const auto table = assign_or_raise(arrow::Table::FromRecordBatches(schema_, batches_));
        // Pretty print table
        if (const auto status = arrow::PrettyPrint(*table, {}, stream_.get()); !status.ok()) {
            throw std::runtime_error("Error printing table: " + status.ToString());
        }
        stream_->flush();
        batches_.clear();
    }

private:
    static std::shared_ptr<std::ostream> open_output_stream(const std::string &path) {
        const auto stream_ptr = std::make_shared<std::ofstream>(path);
        if (!stream_ptr->is_open()) {
            throw std::runtime_error("Unable to open file " + path + " for writing.");
        }
        return std::static_pointer_cast<std::ostream>(stream_ptr);
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<std::ostream> stream_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};
