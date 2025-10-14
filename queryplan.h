#pragma once

#include <iostream>
#include <optional>
#include <sstream>

#include <query.h>

struct ParameterisedQuery {
    std::string query;
    std::vector<ColumnQueryParam> params;
};

struct QueryPlan {
    std::optional<SelectFragment> select;
    std::optional<JoinFragment> join;
    std::optional<WhereFragment> where;
    std::optional<LimitFragment> limit;
    std::optional<OrderFragment> order;
    std::optional<SqlFragment> sql;
    std::uint32_t next_alias_id {0};

    [[nodiscard]] std::optional<ParameterisedQuery> generate_query(AliasGenerator &alias_generator) const {
        if (sql) {
            return ParameterisedQuery{.query = sql->get_fragment(alias_generator), .params = {}};
        }

        if (!select) {
            std::cerr << "No 'SELECT' clause present in query plan.\n";
            return std::nullopt;
        }

        std::stringstream query_buf;
        std::vector<ColumnQueryParam> parameters;

        accumulate(query_buf, parameters, select, alias_generator);
        accumulate(query_buf, parameters, join, alias_generator);
        accumulate(query_buf, parameters, where, alias_generator);
        accumulate(query_buf, parameters, order, alias_generator);
        accumulate(query_buf, parameters, limit, alias_generator);

        ParameterisedQuery query{.query = query_buf.str(), .params = parameters};
        return query;
    }

private:
    template<typename T>
    static void accumulate(
        std::stringstream &query_buf,
        std::vector<ColumnQueryParam> &parameters,
        const std::optional<T> &fragment,
        AliasGenerator &alias_generator
    ) {
        if (fragment) {
            accumulate(query_buf, parameters, *fragment, alias_generator);
        }
    }

    static void accumulate(
        std::stringstream &query_buf,
        std::vector<ColumnQueryParam> &parameters,
        const QueryFragment &fragment,
        AliasGenerator &alias_generator
    ) {
        query_buf << fragment.get_fragment(alias_generator);
        auto params = fragment.get_params();
        parameters.insert(parameters.end(), params.begin(), params.end());
    }
};
