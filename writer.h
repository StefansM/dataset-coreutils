#ifndef WRITER_H_
#define WRITER_H_

#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/io/api.h>

#include "arrow_result.h"

class Writer {
public:
    virtual void write(std::shared_ptr<arrow::RecordBatch> batch) = 0;
    virtual ~Writer() = default;
};

class ArrowDatasetWriter : public Writer {
protected:
    virtual ~ArrowDatasetWriter() = default;

    ArrowDatasetWriter(
            std::shared_ptr<arrow::Schema> schema,
            std::string path,
            std::shared_ptr<arrow::dataset::FileFormat> file_format) {

        auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
        auto file_options = file_format->DefaultWriteOptions();
        auto stream = assign_or_raise(arrow::io::FileOutputStream::Open(path));

        writer_ = assign_or_raise(
            file_format->MakeWriter(stream, schema, file_options, {fs, path})
        );
    }

public:
    void write(std::shared_ptr<arrow::RecordBatch> batch) override {
        writer_->Write(batch);
    }

private:
    std::shared_ptr<arrow::dataset::FileWriter> writer_;
};

class ParquetWriter : public ArrowDatasetWriter {
public:
    virtual ~ParquetWriter() = default;

    ParquetWriter(std::shared_ptr<arrow::Schema> schema, std::string path)
        : ArrowDatasetWriter(
                schema,
                path,
                std::make_shared<arrow::dataset::ParquetFileFormat>()
          ) {}
};

class CsvWriter : public ArrowDatasetWriter{
public:
    virtual ~CsvWriter() = default;

    CsvWriter(std::shared_ptr<arrow::Schema> schema, std::string path)
        : ArrowDatasetWriter(
                schema,
                path,
                std::make_shared<arrow::dataset::CsvFileFormat>()
          ) {}
};

#endif /* WRITER_H_ */

