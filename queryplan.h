#pragma once

#include <iostream>
#include <optional>
#include <sstream>

#include <query.h>

struct ParameterisedQuery {
    std::string query;
    std::vector<QueryParam> params;
};

struct QueryPlan {
    std::optional<SelectFragment> select;
    std::optional<WhereFragment> where;
    std::optional<LimitFragment> limit;
    std::optional<OrderFragment> order;
    std::optional<SqlFragment> sql;

    [[nodiscard]] std::optional<ParameterisedQuery> generate_query() const {
        if (sql) {
            return ParameterisedQuery {.query = sql->get_fragment(), .params = {}};
        }

        if (!select) {
            std::cerr << "No 'SELECT' clause present in query plan.\n";
            return std::nullopt;
        }

        std::stringstream query_buf;
        std::vector<QueryParam> parameters;

        accumulate(query_buf, parameters, select);
        accumulate(query_buf, parameters, where);
        accumulate(query_buf, parameters, order);
        accumulate(query_buf, parameters, limit);

        ParameterisedQuery query{.query = query_buf.str(), .params = parameters};
        return query;
    }

private:
    template<typename T>
    static void accumulate(std::stringstream &query_buf, std::vector<QueryParam> &parameters,
                           const std::optional<T> &fragment) {

        if (fragment) {
            accumulate(query_buf, parameters, *fragment);
        }
    }

    static void accumulate(std::stringstream &query_buf, std::vector<QueryParam> &parameters,
                           const QueryFragment &fragment) {

        query_buf << fragment.get_fragment();
        auto params = fragment.get_params();
        parameters.insert(parameters.end(), params.begin(), params.end());
    }
};
