#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

enum class ParamType : std::uint8_t { NUMERIC, TEXT, UNKNOWN };

using TypeMap = std::map<std::string, ParamType>;

class AliasGenerator {
public:
    explicit AliasGenerator(
        std::string prefix = "t"
    ) :
        prefix_(std::move(prefix)) {}

    std::string next() {
        return prefix_ + std::to_string(counter_++);
    }

private:
    std::string prefix_;
    std::uint32_t counter_ = 1;
};

class QueryParam {
public:
    explicit QueryParam(
        std::string text
    );

    explicit QueryParam(
        std::int64_t number
    );

    static QueryParam unknown(
        std::string text
    );

    template<typename T>
    [[nodiscard]] T get() const;

    [[nodiscard]] bool is_null() const;

    [[nodiscard]] ParamType type() const;

private:
    QueryParam(
        std::string text,
        ParamType type
    );

    ParamType type_;
    std::variant<std::string, std::int64_t> value_;
};

std::ostream& operator<<(std::ostream& os, const QueryParam& param);

struct ColumnQueryParam {
    std::string column;
    QueryParam value;
};


class QueryFragment {
public:
    [[nodiscard]] virtual std::string get_fragment(
        AliasGenerator &alias_generator
    ) const = 0;

    [[nodiscard]] virtual std::vector<ColumnQueryParam> get_params() const {
        return {};
    }

    QueryFragment() = default;

    virtual ~QueryFragment() = default;

    QueryFragment(
        const QueryFragment &
    ) = default;

    QueryFragment &operator=(
        const QueryFragment &
    ) = default;

    QueryFragment(
        QueryFragment &&
    ) = default;

    QueryFragment &operator=(
        QueryFragment &&
    ) = default;
};

class SelectFragment final : public QueryFragment {
public:
    SelectFragment(
        std::vector<std::string> tablenames,
        std::vector<std::string> columns,
        std::optional<std::string> alias
    );

    [[nodiscard]] std::string get_fragment(
        AliasGenerator &alias_generator
    ) const override;

    [[nodiscard]] std::vector<std::string> get_tablenames() const;

    [[nodiscard]] std::vector<std::string> get_columns() const;

    [[nodiscard]] std::optional<std::string> get_alias() const;

private:
    std::vector<std::string> tablenames_;
    std::vector<std::string> columns_;
    std::optional<std::string> alias_;

    [[nodiscard]] std::string fragment_for_single_table(
        const std::string &tablename,
        const std::optional<std::string> &alias
    ) const;
};

struct Condition {
    std::string column;
    std::string predicate;
    QueryParam value;
};

class WhereFragment final : public QueryFragment {
public:
    WhereFragment();

    void add_condition(
        std::string column,
        std::string predicate,
        QueryParam value
    );

    [[nodiscard]] std::vector<Condition> get_conditions() const;

    [[nodiscard]] std::string get_fragment(
        AliasGenerator &alias_generator
    ) const override;

    [[nodiscard]] std::vector<ColumnQueryParam> get_params() const override;

private:
    std::vector<Condition> conditions_;
};

class LimitFragment final : public QueryFragment {
public:
    explicit LimitFragment(
        std::uint32_t limit
    );

    [[nodiscard]] std::string get_fragment(
        AliasGenerator &alias_generator
    ) const override;

    [[nodiscard]] std::uint32_t get_limit() const;

private:
    std::uint32_t limit_;
};

class OrderFragment final : public QueryFragment {
public:
    OrderFragment(
        std::vector<std::string> columns,
        bool reverse
    );

    [[nodiscard]] std::string get_fragment(
        AliasGenerator &alias_generator
    ) const override;

    [[nodiscard]] std::vector<std::string> get_columns() const;

    [[nodiscard]] bool reversed() const;

private:
    std::vector<std::string> columns_;
    bool reverse_;
};


class SqlFragment final : public QueryFragment {
public:
    explicit SqlFragment(
        std::string sql
    );

    [[nodiscard]] std::string get_fragment(
        AliasGenerator &alias_generator
    ) const override;

    [[nodiscard]] std::string get_sql() const;

private:
    std::string sql_;
};

struct JoinCondition {
    std::string left, predicate, right;
};

class JoinFragment final : public QueryFragment {
public:
    explicit JoinFragment(
        std::string table,
        std::string how,
        std::vector<JoinCondition> conditions,
        std::optional<std::string> alias
    );

    [[nodiscard]] std::string get_fragment(
        AliasGenerator &alias_generator
    ) const override;

    [[nodiscard]] std::string get_how() const;

    [[nodiscard]] std::string get_table() const;

    [[nodiscard]] std::vector<JoinCondition> get_conditions() const;

    [[nodiscard]] std::optional<std::string> get_alias() const;

private:
    std::string table_, how_;
    std::vector<JoinCondition> conditions_;
    std::optional<std::string> alias_;
};
