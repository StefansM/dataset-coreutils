#include <fstream>

#include "arrow_result.h"

#include "writer.h"


std::shared_ptr<arrow::io::OutputStream> open_stdout_stream() {
    int const stdout_fd = fileno(stdout);
    if (stdout_fd == -1) {
        throw std::runtime_error("Unable to obtain file number of stdout.");
    }
    return assign_or_raise(arrow::io::FileOutputStream::Open(stdout_fd));
}

std::shared_ptr<std::ostream> non_owning_stdout() {
    return {
        &std::cout,
        [](
    std::ostream *
) {}
    };
}


ArrowDatasetWriter::ArrowDatasetWriter(
    std::shared_ptr<arrow::Schema> schema,
    const std::string &path,
    const std::shared_ptr<arrow::dataset::FileFormat> &file_format
) :
    ArrowDatasetWriter(std::move(schema), file_format, assign_or_raise(arrow::io::FileOutputStream::Open(path))) {}

ArrowDatasetWriter::ArrowDatasetWriter(
    std::shared_ptr<arrow::Schema> schema,
    const std::shared_ptr<arrow::dataset::FileFormat> &file_format
) :
    ArrowDatasetWriter(std::move(schema), file_format, open_stdout_stream()) {}

ArrowDatasetWriter::ArrowDatasetWriter(
    std::shared_ptr<arrow::Schema> schema,
    const std::shared_ptr<arrow::dataset::FileFormat> &file_format,
    std::shared_ptr<arrow::io::OutputStream> stream
) {
    const auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    const auto file_options = file_format->DefaultWriteOptions();

    // The "path" parameter in the FileLocator member doesn't seem to be used for anything.
    writer_ = assign_or_raise(
        file_format->MakeWriter(std::move(stream), std::move(schema), file_options, {.filesystem = fs, .path = ""})
    );
}


void ArrowDatasetWriter::write(
    const std::shared_ptr<arrow::RecordBatch> batch
) {
    if (const auto status = writer_->Write(batch); !status.ok()) {
        throw std::runtime_error("Error writing batch: " + status.ToString());
    }
}


ParquetWriter::ParquetWriter(
    std::shared_ptr<arrow::Schema> schema,
    const std::string &path
) :
    ArrowDatasetWriter(std::move(schema), path, std::make_shared<file_format>()) {}

ColumnarWriter::ColumnarWriter(
    std::shared_ptr<arrow::Schema> schema
) :
    schema_(std::move(schema)),
    stream_(non_owning_stdout()),
    print_options_(print_options()) {
    init();
}

ColumnarWriter::ColumnarWriter(
    std::shared_ptr<arrow::Schema> schema,
    const std::string &path
) :
    schema_(std::move(schema)),
    stream_(open_output_stream(path)),
    print_options_(print_options()) {
    init();
}

void ColumnarWriter::write(
    std::shared_ptr<arrow::RecordBatch> batch
) {
    const auto cols = batch->columns();

    std::int64_t num_rows = 0;
    if (cols.empty()) {
        throw std::runtime_error("No columns were provided");
    }
    num_rows = cols[0]->length();

    std::stringstream stream;
    std::vector<std::string> row;

    for (std::int64_t i = 0; i < num_rows; ++i) {
        for (std::size_t j = 0; j < cols.size(); ++j) {
            const auto& column = cols[j];
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

void ColumnarWriter::flush() {
    for (const auto &row: rendered_rows_) {
        for (std::size_t i = 0; i < row.size(); ++i) {
            const auto width = std::in_range<int>(max_col_width_[i])
                    ? static_cast<int>(max_col_width_[i])
                    : std::numeric_limits<int>::max();

            const auto *delimiter = i == 0 ? "" : " ";
            *stream_ << delimiter << std::left << std::setw(width);
            *stream_ << row[i];
        }
        *stream_ << '\n';
    }
}

arrow::PrettyPrintOptions ColumnarWriter::print_options() {
    arrow::PrettyPrintOptions print_options{};
    print_options.show_field_metadata = false;
    print_options.array_delimiters = {.open = "", .close = ""};
    print_options.indent_size = 0;
    print_options.skip_new_lines = true;
    return print_options;
}

void ColumnarWriter::init() {
    std::vector<std::string> header;
    for (const auto &field: schema_->fields()) {
        const auto name = field->name();
        header.push_back(name);
        max_col_width_.push_back(name.size());
    }
    rendered_rows_.push_back(header);
}

std::shared_ptr<std::ostream> ColumnarWriter::open_output_stream(
    const std::string &path
) {
    const auto stream_ptr = std::make_shared<std::ofstream>(path);
    if (!stream_ptr->is_open()) {
        throw std::runtime_error("Unable to open file " + path + " for writing.");
    }
    return std::static_pointer_cast<std::ostream>(stream_ptr);
}

std::unique_ptr<Writer> default_writer(
    const std::shared_ptr<arrow::Schema> &schema
) {
    return std::make_unique<CsvWriter>(schema);
}
