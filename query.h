#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <variant>
#include <vector>

enum class ParamType : std::uint8_t { NUMERIC, TEXT, UNKNOWN };

using TypeMap = std::map<std::string, ParamType>;

class QueryParam {
public:
    explicit QueryParam(std::string text);

    explicit QueryParam(std::int64_t number);

    static QueryParam unknown(std::string text);

    template<typename T>
    [[nodiscard]] T get() const;

    bool is_null() const;

    [[nodiscard]] ParamType type() const;

private:
    QueryParam(std::string text, ParamType type);

    ParamType type_;
    std::variant<std::string, std::int64_t> value_;
};

struct ColumnQueryParam {
    std::string column;
    QueryParam value;
};


class QueryFragment {
public:
    [[nodiscard]] virtual std::string get_fragment() const = 0;

    [[nodiscard]] virtual std::vector<ColumnQueryParam> get_params() const { return {}; }

    QueryFragment() = default;
    virtual ~QueryFragment() = default;
    QueryFragment(const QueryFragment &) = default;
    QueryFragment &operator=(const QueryFragment &) = default;
    QueryFragment(QueryFragment &&) = default;
    QueryFragment &operator=(QueryFragment &&) = default;
};

class SelectFragment final : public QueryFragment {
public:
    SelectFragment(std::string tablename, std::vector<std::string> columns);

    [[nodiscard]] std::string get_fragment() const override;

    [[nodiscard]] std::string get_tablename() const;
    [[nodiscard]] std::vector<std::string> get_columns() const;

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
    WhereFragment();
    void add_condition(std::string column, std::string predicate, QueryParam value);
    [[nodiscard]] std::vector<Condition> get_conditions() const;
    [[nodiscard]] std::string get_fragment() const override;
    [[nodiscard]] std::vector<ColumnQueryParam> get_params() const override;

private:
    std::vector<Condition> conditions_;
};

class LimitFragment final : public QueryFragment {
public:
    explicit LimitFragment(std::uint32_t limit);

    [[nodiscard]] std::string get_fragment() const override;

    [[nodiscard]] std::uint32_t get_limit() const;

private:
    std::uint32_t limit_;
};

class OrderFragment final : public QueryFragment {
public:
    OrderFragment(std::vector<std::string> columns, bool reverse);

    [[nodiscard]] std::string get_fragment() const override;

    [[nodiscard]] std::vector<std::string> get_columns() const;
    [[nodiscard]] bool reversed() const;

private:
    std::vector<std::string> columns_;
    bool reverse_;
};


class SqlFragment final : public QueryFragment {
public:
    explicit SqlFragment(std::string sql);

    [[nodiscard]] std::string get_fragment() const override;

private:
    std::string sql_;
};
