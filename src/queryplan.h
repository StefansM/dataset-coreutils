#pragma once

#include <iostream>
#include <optional>
#include <sstream>

#include <query.h>
#include <arrow/type_traits.h>

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
    std::uint32_t next_alias_id{0};

    [[nodiscard]] std::optional<ParameterisedQuery> generate_query(
        AliasGenerator &alias_generator
    ) const {
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

class OverallQueryPlan final {
public:
    void add_plan(
        const QueryPlan &plan
    ) {
        plans_.push_back(plan);
    }

    [[nodiscard]] std::vector<QueryPlan>& get_plans() {
        return plans_;
    }

    [[nodiscard]] const std::vector<QueryPlan>& get_plans() const {
        return plans_;
    }

    [[nodiscard]] std::optional<ParameterisedQuery> generate_query(
        AliasGenerator &alias_generator
    ) const {
        std::stringstream query_buf;
        std::vector<ColumnQueryParam> parameters;

        std::size_t i = 0;

        for (const auto &plan : plans_) {
            const auto is_cte = i < plans_.size() - 1;

            if (is_cte) {
                if (i == 0) {
                    query_buf << "WITH ";
                } else {
                    query_buf << ", ";
                }
                const auto alias = plan.select && plan.select->get_alias() ? plan.select->get_alias() : std::nullopt;
                const auto cte_name = alias.value_or(alias_generator.next());
                query_buf << cte_name << " AS (\n";
            }

            const auto param_query = plan.generate_query(alias_generator);
            if (!param_query) {
                std::cerr << "Error generating query from plan " << i << ".\n";
                return std::nullopt;
            }

            query_buf << param_query->query;
            parameters.insert(parameters.end(), param_query->params.begin(), param_query->params.end());

            if (is_cte) {
                query_buf << "\n)\n";
            }

            i++;
        }
        return ParameterisedQuery{.query = query_buf.str(), .params = parameters};
    }

private:
    std::vector<QueryPlan> plans_;
};
