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

class ParquetWriter : public Writer {
public:
    virtual ~ParquetWriter() = default;

    ParquetWriter(std::shared_ptr<arrow::Schema> schema, std::string path) {
        auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
        auto file_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
        auto file_options = file_format->DefaultWriteOptions();
        auto stream = assign_or_raise(arrow::io::FileOutputStream::Open(path));

        writer_ = assign_or_raise(
            file_format->MakeWriter(stream, schema, file_options, {fs, path})
        );
    }

    void write(std::shared_ptr<arrow::RecordBatch> batch) override {
        writer_->Write(batch);
    }

private:
    std::shared_ptr<arrow::dataset::FileWriter> writer_;
};

#endif /* WRITER_H_ */

