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
            std::shared_ptr<arrow::dataset::FileFormat> file_format) 
        : ArrowDatasetWriter(
                schema,
                file_format,
                assign_or_raise(arrow::io::FileOutputStream::Open(path))
          ) {}

    ArrowDatasetWriter(
            std::shared_ptr<arrow::Schema> schema,
            int fd,
            std::shared_ptr<arrow::dataset::FileFormat> file_format)
        : ArrowDatasetWriter(
                schema,
                file_format,
                assign_or_raise(arrow::io::FileOutputStream::Open(fd))
          ) {}


    ArrowDatasetWriter(
            std::shared_ptr<arrow::Schema> schema,
            std::shared_ptr<arrow::dataset::FileFormat> file_format,
            std::shared_ptr<arrow::io::OutputStream> stream) {

        auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
        auto file_options = file_format->DefaultWriteOptions();

        // The "path" parameter in the FileLocator member doesn't seem to be used for anything.
        writer_ = assign_or_raise(
            file_format->MakeWriter(stream, schema, file_options, {fs, ""})
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
    using file_format = arrow::dataset::ParquetFileFormat;

public:
    virtual ~ParquetWriter() = default;

    ParquetWriter(std::shared_ptr<arrow::Schema> schema, std::string path)
        : ArrowDatasetWriter(schema, path, std::make_shared<file_format>())
    {}

    ParquetWriter(std::shared_ptr<arrow::Schema> schema, int fd)
        : ArrowDatasetWriter(schema, fd, std::make_shared<file_format>())
    {}
};

class CsvWriter : public ArrowDatasetWriter{
    using file_format = arrow::dataset::CsvFileFormat;

public:
    virtual ~CsvWriter() = default;

    CsvWriter(std::shared_ptr<arrow::Schema> schema, std::string path)
        : ArrowDatasetWriter(schema, path, std::make_shared<file_format>())
    {}

    CsvWriter(std::shared_ptr<arrow::Schema> schema, int fd)
        : ArrowDatasetWriter(schema, fd, std::make_shared<file_format>())
    {}
};

#endif /* WRITER_H_ */

