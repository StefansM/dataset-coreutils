#include "serde.h"

#include <cstdio>
#include <iostream>
#include <optional>
#include <unistd.h>

#include <json/json.h>

#include "query.h"
#include "query_evaluator.h"
#include "queryplan.h"
#include "writer.h"

static void dump_json(
    const Json::Value &value,
    std::ostream &out
);

static std::optional<Json::Value> load_json(
    std::istream &in
);

Json::Value QueryParamSerDes::encode(
    const QueryParam &fragment
) {
    Json::Value value;
    switch (fragment.type()) {
        case ParamType::NUMERIC:
            value["type"] = "NUMERIC";
            value["value"] = fragment.get<std::int64_t>();
            break;
        case ParamType::TEXT:
            value["type"] = "TEXT";
            value["value"] = fragment.get<std::string>();
            break;
        case ParamType::UNKNOWN:
            value["type"] = "UNKNOWN";
            value["value"] = fragment.get<std::string>();
            break;
    }
    return value;
}

QueryParam QueryParamSerDes::decode(
    const Json::Value &fragment
) {
    const auto type = fragment["type"].asString();
    const auto &value = fragment["value"];
    if (type == "NUMERIC") {
        return QueryParam(value.asInt64());
    }
    if (type == "TEXT") {
        return QueryParam(value.asString());
    }
    return QueryParam::unknown(value.asString());
}

Json::Value SelectSerDes::encode(
    const SelectFragment &fragment
) {
    Json::Value value;
    for (const auto &tn: fragment.get_tablenames()) {
        value["tablename"].append(tn);
    }
    value["alias"] = fragment.get_alias() ? *fragment.get_alias() : Json::Value::null;

    int i = 0;
    for (const auto &col: fragment.get_columns()) {
        value["columns"][i++] = col;
    }

    return value;
}

SelectFragment SelectSerDes::decode(
    const Json::Value &fragment
) {
    std::vector<std::string> tablenames;
    for (const auto &tn: fragment["tablename"]) {
        tablenames.push_back(tn.asString());
    }

    std::vector<std::string> columns;
    for (const auto &col: fragment["columns"]) {
        columns.push_back(col.asString());
    }
    const auto &alias_value = fragment["alias"];
    const auto alias_opt = alias_value != Json::Value::null ? std::make_optional(alias_value.asString()) : std::nullopt;

    return {tablenames, columns, alias_opt};
}

Json::Value WhereSerDes::encode(
    const WhereFragment &fragment
) {
    Json::Value value;
    value["conditions"] = Json::Value();

    int i = 0;
    for (const auto &[column, predicate, param]: fragment.get_conditions()) {
        Json::Value json_cond;
        json_cond["column"] = column;
        json_cond["predicate"] = predicate;
        json_cond["value"] = QueryParamSerDes::encode(param);

        value["conditions"][i++] = json_cond;
    }

    return value;
}

WhereFragment WhereSerDes::decode(
    const Json::Value &json
) {
    WhereFragment fragment;
    for (const auto &cond: json["conditions"]) {
        const auto column = cond["column"].asString();
        const auto predicate = cond["predicate"].asString();
        const auto value = QueryParamSerDes::decode(cond["value"]);

        fragment.add_condition(column, predicate, value);
    }

    return fragment;
}

Json::Value LimitSerDes::encode(
    const LimitFragment &fragment
) {
    Json::Value value;
    value["limit"] = fragment.get_limit();
    return value;
}

LimitFragment LimitSerDes::decode(
    const Json::Value &json
) {
    return LimitFragment{json["limit"].asUInt()};
}

Json::Value OrderSerDes::encode(
    const OrderFragment &fragment
) {
    Json::Value value;

    value["reversed"] = fragment.reversed();
    value["fields"] = Json::Value();

    int i = 0;
    for (const auto &f: fragment.get_columns()) {
        value["fields"][i++] = f;
    }

    return value;
}

OrderFragment OrderSerDes::decode(
    const Json::Value &json
) {
    std::vector<std::string> columns;
    for (const auto &cond: json["fields"]) {
        columns.push_back(cond.asString());
    }

    return {columns, json["reversed"].asBool()};
}

Json::Value SqlSerDes::encode(
    const SqlFragment &fragment
) {
    Json::Value value;

    value["sql"] = fragment.get_sql();

    return value;
}

SqlFragment SqlSerDes::decode(
    const Json::Value &json
) {
    return SqlFragment{json["sql"].asString()};
}

Json::Value JoinSerDes::encode(
    const JoinFragment &fragment
) {
    Json::Value value;
    value["table"] = fragment.get_table();
    value["alias"] = fragment.get_alias() ? *fragment.get_alias() : Json::Value::null;
    value["how"] = fragment.get_how();
    value["conditions"] = Json::Value();

    int i = 0;
    for (const auto &f: fragment.get_conditions()) {
        Json::Value json_cond;
        json_cond["left"] = f.left;
        json_cond["predicate"] = f.predicate;
        json_cond["right"] = f.right;
        value["conditions"][i++] = json_cond;
    }
    return value;
}

JoinFragment JoinSerDes::decode(
    const Json::Value &json
) {
    const std::string how = json["how"].asString();
    const std::string table = json["table"].asString();
    const auto &alias_value = json["alias"];
    const auto alias_opt = alias_value != Json::Value::null ? std::make_optional(alias_value.asString()) : std::nullopt;

    std::vector<JoinCondition> conditions;
    for (const auto &cond: json["conditions"]) {
        conditions.push_back(
            JoinCondition{
                .left = cond["left"].asString(),
                .predicate = cond["predicate"].asString(),
                .right = cond["right"].asString()
            }
        );
    }
    return JoinFragment{table, how, conditions, alias_opt};
}

Json::Value QueryPlanSerDes::encode(
    const QueryPlan &query_plan
) {
    Json::Value root;
    root["select"] = Json::Value::null;
    root["where"] = Json::Value::null;
    root["limit"] = Json::Value::null;
    root["order"] = Json::Value::null;
    root["sql"] = Json::Value::null;
    root["join"] = Json::Value::null;

    if (query_plan.select) {
        root["select"] = SelectSerDes::encode(*query_plan.select);
    }

    if (query_plan.where) {
        root["where"] = WhereSerDes::encode(*query_plan.where);
    }

    if (query_plan.limit) {
        root["limit"] = LimitSerDes::encode(*query_plan.limit);
    }

    if (query_plan.order) {
        root["order"] = OrderSerDes::encode(*query_plan.order);
    }

    if (query_plan.sql) {
        root["sql"] = SqlSerDes::encode(*query_plan.sql);
    }

    if (query_plan.join) {
        root["join"] = JoinSerDes::encode(*query_plan.join);
    }

    return root;
}

QueryPlan QueryPlanSerDes::decode(
    const Json::Value &root
) {
    QueryPlan query_plan{};

    if (const auto &select = root["select"]; select != Json::Value::null) {
        query_plan.select = SelectSerDes::decode(select);
    }

    if (const auto &where = root["where"]; where != Json::Value::null) {
        query_plan.where = WhereSerDes::decode(where);
    }

    if (const auto &limit = root["limit"]; limit != Json::Value::null) {
        query_plan.limit = LimitSerDes::decode(limit);
    }

    if (const auto &order = root["order"]; order != Json::Value::null) {
        query_plan.order = OrderSerDes::decode(order);
    }

    if (const auto &sql = root["sql"]; sql != Json::Value::null) {
        query_plan.sql = SqlSerDes::decode(sql);
    }

    if (const auto &join = root["join"]; join != Json::Value::null) {
        query_plan.join = JoinSerDes::decode(join);
    }

    return query_plan;
}

Json::Value OverallQueryPlanSerDes::encode(
    const OverallQueryPlan &overall_query_plan
) {
    Json::Value root;
    root["plans"] = Json::Value();
    int i = 0;
    for (const auto &plan: overall_query_plan.get_plans()) {
        root["plans"][i++] = QueryPlanSerDes::encode(plan);
    }
    return root;
}

OverallQueryPlan OverallQueryPlanSerDes::decode(
    const Json::Value &root
) {
    const auto &plans_json = root["plans"];
    OverallQueryPlan overall_query_plan;
    for (const auto &plan_json: plans_json) {
        overall_query_plan.add_plan(QueryPlanSerDes::decode(plan_json));
    }
    return overall_query_plan;
}


void dump_json(
    const Json::Value &value,
    std::ostream &out
) {
    const Json::StreamWriterBuilder builder;
    const std::unique_ptr<Json::StreamWriter> json_writer(builder.newStreamWriter());
    json_writer->write(value, &out);
}

std::optional<Json::Value> load_json(
    std::istream &in
) {
    Json::Value root;
    JSONCPP_STRING errs;

    if (const Json::CharReaderBuilder builder; !Json::parseFromStream(builder, in, &root, &errs)) {
        std::cerr << errs << '\n';
        return std::nullopt;
    }

    return root;
}

std::optional<OverallQueryPlan> load_query_plan(
    std::istream &in
) {
    const auto json_doc = load_json(in);
    if (!json_doc) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return std::nullopt;
    }

    return OverallQueryPlanSerDes::decode(*json_doc);
}

void dump_query_plan(
    const OverallQueryPlan &query_plan,
    std::ostream &out
) {
    const Json::Value query_plan_encoded = OverallQueryPlanSerDes::encode(query_plan);
    dump_json(query_plan_encoded, out);
}

ExitStatus dump_or_eval_query_plan(
    const OverallQueryPlan &query_plan
) {
    if (isatty(fileno(stdout)) == 1) {
        AliasGenerator alias_generator;
        return evaluate_query(query_plan, default_writer, alias_generator);
    }
    dump_query_plan(query_plan, std::cout);
    return ExitStatus::SUCCESS;
}
