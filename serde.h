#pragma once

#include <optional>

#include "query_evaluator.h"

#include <json/json.h>

struct QueryPlan;
class QueryParam;
class SelectFragment;
class WhereFragment;
class LimitFragment;
class OrderFragment;
class SqlFragment;

class QueryParamSerDes final {
public:
    static Json::Value encode(const QueryParam &fragment);

    static QueryParam decode(const Json::Value &fragment);
};

class SelectSerDes final {
public:
    static Json::Value encode(const SelectFragment &fragment);

    static SelectFragment decode(const Json::Value &fragment);
};

class WhereSerDes final {
public:
    static Json::Value encode(const WhereFragment &fragment);

    static WhereFragment decode(const Json::Value &json);
};

class LimitSerDes final {
public:
    static Json::Value encode(const LimitFragment &fragment);

    static LimitFragment decode(const Json::Value &json);
};

class OrderSerDes final {
public:
    static Json::Value encode(const OrderFragment &fragment);

    static OrderFragment decode(const Json::Value &json);
};

class SqlSerDes final {
public:
    static Json::Value encode(const SqlFragment &fragment);

    static SqlFragment decode(const Json::Value &json);
};

class QueryPlanSerDes final {
public:
    static Json::Value encode(const QueryPlan &query_plan);

    static QueryPlan decode(const Json::Value &root);
};

std::optional<QueryPlan> load_query_plan(std::istream &in);
void dump_query_plan(const QueryPlan &query_plan, std::ostream &out);
ExitStatus dump_or_eval_query_plan(const QueryPlan &query_plan);
