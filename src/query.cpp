#include "query.h"

#include <optional>
#include <sstream>

// QueryParam
QueryParam::QueryParam(
    std::string text
) :
    QueryParam(std::move(text), ParamType::TEXT) {}

QueryParam::QueryParam(
    std::int64_t number
) :
    type_(ParamType::NUMERIC),
    value_(number) {}

QueryParam QueryParam::unknown(
    std::string text
) {
    return {std::move(text), ParamType::UNKNOWN};
}

QueryParam::QueryParam(
    std::string text,
    const ParamType type
) :
    type_(type),
    value_(std::move(text)) {}

std::ostream & operator<<(
    std::ostream &os,
    const QueryParam &param
) {
    switch (param.type()) {
        case ParamType::NUMERIC:
            os << param.get<std::int64_t>() << "(INTEGER)";
            break;
        case ParamType::TEXT:
            os << param.get<std::string>() << "(TEXT)";
            break;
        case ParamType::UNKNOWN:
            os << param.get<std::string>() << "(UNKNOWN)";
            break;
    }
    return os;
}

bool QueryParam::is_null() const {
    return type_ == ParamType::UNKNOWN && std::get<std::string>(value_) == "NULL";
}

ParamType QueryParam::type() const {
    return type_;
}

template<typename T>
T QueryParam::get() const {
    return std::get<T>(value_);
}

// Available specialisations for get()
template std::string QueryParam::get<std::string>() const;

template std::int64_t QueryParam::get<std::int64_t>() const;


// SelectFragment
SelectFragment::SelectFragment(
    std::string tablename,
    std::vector<std::string> columns,
    std::optional<std::string> alias
) :
    tablename_(std::move(tablename)),
    columns_(std::move(columns)),
    alias_(std::move(alias)) {}

std::string SelectFragment::get_fragment(
    AliasGenerator &alias_generator
) const {
    std::stringstream stream;
    stream << "SELECT ";

    int i = 0;
    for (const auto &col: columns_) {
        if (i++ != 0) {
            stream << "     , ";
        }
        stream << col << "\n";
    }

    stream << "  FROM " << tablename_;

    const auto alias = alias_.value_or(alias_generator.next());
    stream << " AS " << alias;

    return stream.str();
}

std::string SelectFragment::get_tablename() const {
    return tablename_;
}

std::vector<std::string> SelectFragment::get_columns() const {
    return columns_;
}

std::optional<std::string> SelectFragment::get_alias() const {
    return alias_;
}

// WhereFragment
WhereFragment::WhereFragment() = default;

void WhereFragment::add_condition(
    std::string column,
    std::string predicate,
    QueryParam value
) {
    conditions_.push_back({std::move(column), std::move(predicate), std::move(value)});
}

std::vector<Condition> WhereFragment::get_conditions() const {
    return conditions_;
}

std::string WhereFragment::get_fragment(
    AliasGenerator &
) const {
    std::stringstream stream;
    int i = 0;
    for (const auto &c: conditions_) {
        if (i++ == 0) {
            stream << "\n WHERE ";
        } else {
            stream << "\n   AND ";
        }

        if (c.value.is_null()) {
            stream << c.column << " " << c.predicate << " NULL";
        } else {
            stream << c.column << " " << c.predicate << " ?";
        }
    }
    return stream.str();
}

std::vector<ColumnQueryParam> WhereFragment::get_params() const {
    std::vector<ColumnQueryParam> params;
    params.reserve(conditions_.size());

    for (const auto &c: conditions_) {
        if (!c.value.is_null()) {
            params.emplace_back(ColumnQueryParam{.column = c.column, .value = {c.value}});
        }
    }

    return params;
}

// LimitFragment
LimitFragment::LimitFragment(
    const std::uint32_t limit
) :
    limit_(limit) {}

std::string LimitFragment::get_fragment(
    AliasGenerator &
) const {
    std::stringstream stream;
    // FIXME: When using a query parameter for limit across multiple Parquet
    // files, I get an incorrect result compared to doing this.
    stream << "\n LIMIT " << limit_;
    return stream.str();
}

std::uint32_t LimitFragment::get_limit() const {
    return limit_;
}

// OrderFragment
OrderFragment::OrderFragment(
    std::vector<std::string> columns,
    const bool reverse
) :
    columns_(std::move(columns)),
    reverse_(reverse) {}

std::string OrderFragment::get_fragment(
    AliasGenerator &
) const {
    const char *direction = reverse_ ? "DESC" : "ASC";

    std::stringstream stream;
    stream << "\n ORDER BY ";

    int i = 0;
    for (const auto &col: columns_) {
        if (i++ != 0) {
            stream << "\n     , ";
        }
        stream << col << " " << direction;
    }
    return stream.str();
}

std::vector<std::string> OrderFragment::get_columns() const {
    return columns_;
}

bool OrderFragment::reversed() const {
    return reverse_;
}

// SqlFragment
SqlFragment::SqlFragment(
    std::string sql
) :
    sql_(std::move(sql)) {}

std::string SqlFragment::get_fragment(
    AliasGenerator &
) const {
    return sql_;
}

std::string SqlFragment::get_sql() const {
    return sql_;
}

JoinFragment::JoinFragment(
    std::string table,
    std::string how,
    std::vector<JoinCondition> conditions,
    std::optional<std::string> alias
) :
    table_(std::move(table)),
    how_(std::move(how)),
    conditions_(std::move(conditions)),
    alias_(std::move(alias)) {}

std::string JoinFragment::get_fragment(
    AliasGenerator &alias_generator
) const {
    std::stringstream stream;
    stream << "\n " << how_ << " JOIN " << table_;

    const auto alias = alias_.value_or(alias_generator.next());
    stream << " AS " << alias;

    stream << " ON ";

    int i = 0;
    for (const auto &[left, predicate, right]: conditions_) {
        if (i++ != 0) {
            stream << "\n     AND ";
        }
        stream << left << " " << predicate << " " << right;
    }

    return stream.str();
}

std::string JoinFragment::get_how() const {
    return how_;
}

std::string JoinFragment::get_table() const {
    return table_;
}

std::vector<JoinCondition> JoinFragment::get_conditions() const {
    return conditions_;
}

std::optional<std::string> JoinFragment::get_alias() const {
    return alias_;
}
