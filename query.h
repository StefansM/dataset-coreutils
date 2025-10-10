#ifndef QUERY_H_
#define QUERY_H_

#include <cstdint>
#include <map>
#include <utility>
#include <variant>
#include <sstream>
#include <string>
#include <algorithm>
#include <vector>

enum class ParamType : std::uint8_t { NUMERIC, TEXT, UNKNOWN };

using TypeMap = std::map<std::string, ParamType>;

class QueryParam {
public:
    explicit QueryParam(std::string text)
        : QueryParam(std::move(text), ParamType::TEXT) {}

    explicit QueryParam(std::int64_t number)
        : type_(ParamType::NUMERIC)
        , value_(number) {}

    static QueryParam unknown(std::string text) {
        return {std::move(text), ParamType::UNKNOWN};
    }

    template <typename T>
    [[nodiscard]] T get() const {
        return std::get<T>(value_);
    }

    [[nodiscard]] ParamType type() const { return type_; }

private:
    QueryParam(std::string text, const ParamType type)
        : type_(type)
        , value_(std::move(text)) {}

    ParamType type_;
    std::variant<std::string, std::int64_t> value_;
};


class QueryFragment {
public:
    [[nodiscard]] virtual std::string get_fragment() const = 0;

    [[nodiscard]] virtual std::vector<QueryParam> get_params() const {
        return {};
    }

    QueryFragment() = default;
    virtual ~QueryFragment() = default;
    QueryFragment (const QueryFragment &) = default;
    QueryFragment& operator= (const QueryFragment &) = default;
    QueryFragment (QueryFragment &&) = default;
    QueryFragment& operator= (QueryFragment &&) = default;
};

class SelectFragment final : public QueryFragment {
public:
    SelectFragment(std::string tablename, std::vector<std::string> columns)
        : tablename_(std::move(tablename))
        , columns_(std::move(columns))
    {}

    [[nodiscard]] std::string get_fragment() const override {
        std::stringstream stream;
        stream << "SELECT ";

        int i = 0;
        for (const auto &col : columns_) {
            if (i++ != 0) {
                stream << "     , ";
            }
            stream << col << "\n";
        }

        stream << "  FROM " << tablename_;
        return stream.str();
    }

    [[nodiscard]] std::string get_tablename() const { return tablename_; }
    [[nodiscard]] std::vector<std::string> get_columns() const { return columns_; }

private:
    std::string tablename_;
    std::vector<std::string> columns_;
};

struct Condition {
    std::string column;
    std::string predicate;
    QueryParam value;
};

class WhereFragment final : public QueryFragment {
public:
    WhereFragment()
    = default;

    void add_condition(std::string column, std::string predicate, QueryParam value) {
        conditions_.push_back({std::move(column), std::move(predicate), std::move(value)});
    }

    [[nodiscard]] std::vector<Condition> get_conditions() const { return conditions_; }

    [[nodiscard]] std::string get_fragment() const override {
        std::stringstream stream;
        int i = 0;
        for (const auto &c : conditions_) {
            if (i++ == 0) {
                stream << "\n WHERE ";
            } else {
                stream << "\n   AND ";
            }
            stream << c.column << " " << c.predicate << " ?";
        }
        return stream.str();
    }

    [[nodiscard]] std::vector<QueryParam> get_params() const override {
        std::vector<QueryParam> params;
        params.reserve(conditions_.size());

        for (const auto &c : conditions_) {
            params.emplace_back(c.value);
        }

        return params;
    }

private:
    std::vector<Condition> conditions_;
};

class LimitFragment final : public QueryFragment {
public:
    explicit LimitFragment(const std::uint32_t limit)
        : limit_(limit)
    {}

    [[nodiscard]] std::string get_fragment() const override {
        std::stringstream stream;
        // FIXME: When using a query parameter for limit across multiple Parquet
        // files, I get an incorrect result compared to doing this.
        stream << "\n LIMIT " << limit_;
        return stream.str();
    }

    [[nodiscard]] std::uint32_t get_limit() const { return limit_; }

private:
    std::uint32_t limit_;
};

class OrderFragment final : public QueryFragment {
public:
    OrderFragment(std::vector<std::string> columns, const bool reverse)
        : columns_(std::move(columns))
        , reverse_(reverse)
    {}

    [[nodiscard]] std::string get_fragment() const override {
        const char *direction = reverse_ ? "DESC" : "ASC";

        std::stringstream stream;
        stream << "\n ORDER BY ";

        int i = 0;
        for (const auto &col : columns_) {
            if (i++ != 0) {
                stream << "\n     , ";
            }
            stream << col << " " << direction;
        }
        return stream.str();
    }

    [[nodiscard]] std::vector<std::string> get_columns() const { return columns_; }
    [[nodiscard]] bool reversed() const { return reverse_; }

private:
    std::vector<std::string> columns_;
    bool reverse_;
};

#endif /* QUERY_H_ */

