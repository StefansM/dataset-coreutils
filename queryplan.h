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
    std::optional<SelectFragment> select = std::nullopt;
    std::optional<WhereFragment> where = std::nullopt;
    virtual ~QueryPlan() = default;

    std::optional<ParameterisedQuery> query() const {
        if (!select) {
            std::cerr << "No 'SELECT' clause present in query plan." << std::endl;
            return std::nullopt;
        }

        std::stringstream query_buf;
        std::vector<QueryParam> parameters;

        query_buf << select->get_fragment();

        // TODO: Generic accumulator
        if (where) {
            query_buf << where->get_fragment();
            auto params = where->get_params();
            parameters.insert(parameters.end(), params.begin(), params.end());
        }

        ParameterisedQuery query {query_buf.str(), parameters};
        return query;
    }
};

#endif /* QUERYPLAN_H_ */

