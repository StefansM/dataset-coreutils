#ifndef QUERY_H_
#define QUERY_H_

#include <cstdint>
#include <variant>
#include <string>
#include <algorithm>

enum class ParamType { NUMERIC, TEXT, UNKNOWN };

using TypeMap = std::map<std::string, ParamType>;

class QueryParam {
public:
    QueryParam(std::string text)
        : type(ParamType::TEXT)
        , value(std::move(text)) {}

    QueryParam(std::int64_t number)
        : type(ParamType::NUMERIC)
        , value(number) {}

    static QueryParam unknown(std::string text) {
        return QueryParam(text, ParamType::UNKNOWN);
    }

    template <typename T>
    T get() const {
        return std::get<T>(value);
    }

    ParamType type;

private:
    QueryParam(std::string text, ParamType type)
        : type(type)
        , value(std::move(text)) {}

    std::variant<std::string, std::int64_t> value;
};


class QueryFragment {
public:
    virtual std::string get_fragment() const = 0;

    virtual std::vector<QueryParam> get_params() const {
        return {};
    }

    virtual ~QueryFragment() = default;
};

class SelectFragment : public QueryFragment {
public:
    SelectFragment(std::string tablename, std::vector<std::string> columns)
        : tablename_(std::move(tablename))
        , columns_(std::move(columns))
    {}

    std::string get_fragment() const override {
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

    std::string get_tablename() const { return tablename_; }
    std::vector<std::string> get_columns() const { return columns_; }

private:
    std::string tablename_;
    std::vector<std::string> columns_;
};

struct Condition {
    std::string column;
    std::string predicate;
    QueryParam value;
};

class WhereFragment : public QueryFragment {
public:
    WhereFragment()
    {}

    void add_condition(std::string column, std::string predicate, QueryParam value) {
        // TODO: Emplace?
        Condition c {column, predicate, value};
        conditions_.push_back(c);
    }

    std::vector<Condition> get_conditions() const { return conditions_; }

    std::string get_fragment() const override {
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

    std::vector<QueryParam> get_params() const override {
        std::vector<QueryParam> params;
        params.reserve(conditions_.size());

        for (const auto &c : conditions_) {
            params.emplace_back(c.value);
        }

        return params;
    }

private:
    std::vector<Condition> conditions_ {};
};

class LimitFragment : public QueryFragment {
public:
    LimitFragment(std::uint64_t limit)
        : limit_(limit)
    {}

    std::string get_fragment() const override {
        std::stringstream stream;
        stream << "LIMIT " << limit_;
        return stream.str();
    }

private:
    std::uint64_t limit_;
};

class OrderFragment : public QueryFragment {
    OrderFragment(std::vector<std::string> columns, bool reverse)
        : columns_(std::move(columns))
        , reverse_(reverse)
    {}

    std::string get_fragment() const override {
        const char *direction = reverse_ ? "DESC" : "ASC";

        std::stringstream stream;
        stream << "\nORDER BY";

        int i = 0;
        for (const auto &col : columns_) {
            if (i++ != 0) {
                stream << "     , ";
            }
            stream << col << " " << direction << "\n";
        }
        return stream.str();
    }

private:
    std::vector<std::string> columns_;
    bool reverse_;
};

#endif /* QUERY_H_ */

