#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include <optional>
#include <iostream>
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

    std::optional<ParameterisedQuery> generate_query() const {
        if (!select) {
            std::cerr << "No 'SELECT' clause present in query plan." << std::endl;
            return std::nullopt;
        }

        std::stringstream query_buf;
        std::vector<QueryParam> parameters;

        accumulate(query_buf, parameters, select);
        accumulate(query_buf, parameters, where);
        accumulate(query_buf, parameters, order);
        accumulate(query_buf, parameters, limit);

        ParameterisedQuery query {query_buf.str(), parameters};
        return query;
    }

private:
    template <typename T>
    void accumulate(
            std::stringstream &query_buf,
            std::vector<QueryParam> &parameters,
            const std::optional<T> &fragment) const {

        if (fragment) {
            accumulate(query_buf, parameters, *fragment);
        }
    }

    void accumulate(
            std::stringstream &query_buf,
            std::vector<QueryParam> &parameters,
            const QueryFragment &fragment) const {

        query_buf << fragment.get_fragment();
        auto params = fragment.get_params();
        parameters.insert(parameters.end(), params.begin(), params.end());
    }

};

#endif /* QUERYPLAN_H_ */

