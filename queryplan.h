#ifndef QUERYPLAN_H_
#define QUERYPLAN_H_

#include <optional>
#include <iostream>
#include <sstream>

#include <query.h>

struct QueryPlan {
    std::optional<SelectFragment> select = std::nullopt;
    std::optional<WhereFragment> where = std::nullopt;
    virtual ~QueryPlan() = default;

    std::optional<std::string> query() const {
        if (!select) {
            std::cerr << "No 'SELECT' clause present in query plan." << std::endl;
            return std::nullopt;
        }

        std::stringstream query_buf;

        query_buf << select->get_fragment();

        if (where) {
            query_buf << where->get_fragment();
        }

        return query_buf.str();
    }
};

#endif /* QUERYPLAN_H_ */

