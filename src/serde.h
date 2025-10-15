#pragma once

#include <optional>

#include "query_evaluator.h"

#include <json/json.h>

struct QueryPlan;
class QueryParam;
class SelectFragment;
class JoinFragment;
class WhereFragment;
class LimitFragment;
class OrderFragment;
class SqlFragment;
class OverallQueryPlan;

class QueryParamSerDes final {
public:
    static Json::Value encode(
        const QueryParam &fragment
    );

    static QueryParam decode(
        const Json::Value &fragment
    );
};

class SelectSerDes final {
public:
    static Json::Value encode(
        const SelectFragment &fragment
    );

    static SelectFragment decode(
        const Json::Value &fragment
    );
};

class WhereSerDes final {
public:
    static Json::Value encode(
        const WhereFragment &fragment
    );

    static WhereFragment decode(
        const Json::Value &json
    );
};

class LimitSerDes final {
public:
    static Json::Value encode(
        const LimitFragment &fragment
    );

    static LimitFragment decode(
        const Json::Value &json
    );
};

class OrderSerDes final {
public:
    static Json::Value encode(
        const OrderFragment &fragment
    );

    static OrderFragment decode(
        const Json::Value &json
    );
};

class SqlSerDes final {
public:
    static Json::Value encode(
        const SqlFragment &fragment
    );

    static SqlFragment decode(
        const Json::Value &json
    );
};

class JoinSerDes final {
public:
    static Json::Value encode(
        const JoinFragment &fragment
    );

    static JoinFragment decode(
        const Json::Value &json
    );
};

class QueryPlanSerDes final {
public:
    static Json::Value encode(
        const QueryPlan &query_plan
    );

    static QueryPlan decode(
        const Json::Value &root
    );
};

class OverallQueryPlanSerDes final {
public:
    static Json::Value encode(
        const OverallQueryPlan &overall_query_plan
    );

    static OverallQueryPlan decode(
        const Json::Value &root
    );
};

std::optional<OverallQueryPlan> load_query_plan(
    std::istream &in
);

void dump_query_plan(
    const OverallQueryPlan &query_plan,
    std::ostream &out
);

ExitStatus dump_or_eval_query_plan(
    const OverallQueryPlan &query_plan
);
