#ifndef SERDE_H_
#define SERDE_H_

#include <iostream>
#include <optional>

#include <json/json.h>

#include "query.h"
#include "queryplan.h"

class QueryParamSerDes {

public:
    static Json::Value encode(const QueryParam &fragment) {
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

    static QueryParam decode(const Json::Value &fragment) {
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
};

class SelectSerDes {
public:
    static Json::Value encode(const SelectFragment &fragment) {
        Json::Value value;
        value["tablename"] = fragment.get_tablename();

        int i = 0;
        for (const auto &col: fragment.get_columns()) {
            value["columns"][i++] = col;
        }

        return value;
    }

    static SelectFragment decode(const Json::Value &fragment) {
        std::vector<std::string> columns;
        for (const auto &col: fragment["columns"]) {
            columns.push_back(col.asString());
        }

        return {fragment["tablename"].asString(), columns};
    }
};

class WhereSerDes {
public:
    static Json::Value encode(const WhereFragment &fragment) {
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

    static WhereFragment decode(const Json::Value &json) {
        WhereFragment fragment;
        for (const auto &cond: json["conditions"]) {
            const auto column = cond["column"].asString();
            const auto predicate = cond["predicate"].asString();
            const auto value = QueryParamSerDes::decode(cond["value"]);

            fragment.add_condition(column, predicate, value);
        }

        return fragment;
    }
};

class LimitSerDes {
public:
    static Json::Value encode(const LimitFragment &fragment) {
        Json::Value value;
        value["limit"] = fragment.get_limit();
        return value;
    }

    static LimitFragment decode(const Json::Value &json) { return LimitFragment{json["limit"].asUInt()}; }
};

class OrderSerDes {
public:
    static Json::Value encode(const OrderFragment &fragment) {
        Json::Value value;

        value["reversed"] = fragment.reversed();
        value["fields"] = Json::Value();

        int i = 0;
        for (const auto &f: fragment.get_columns()) {
            value["fields"][i++] = f;
        }

        return value;
    }

    static OrderFragment decode(const Json::Value &json) {
        std::vector<std::string> columns;
        for (const auto &cond: json["fields"]) {
            columns.push_back(cond.asString());
        }

        return {columns, json["reversed"].asBool()};
    }
};

class QueryPlanSerDes {
public:
    static Json::Value encode(const QueryPlan &query_plan) {
        Json::Value root;
        root["select"] = Json::Value::null;
        root["where"] = Json::Value::null;
        root["limit"] = Json::Value::null;
        root["order"] = Json::Value::null;

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

        return root;
    }

    static QueryPlan decode(const Json::Value &root) {
        QueryPlan query_plan;

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

        return query_plan;
    }
};

inline void dump_json(const Json::Value &value, std::ostream &out) {
    const Json::StreamWriterBuilder builder;
    const std::unique_ptr<Json::StreamWriter> json_writer(builder.newStreamWriter());
    json_writer->write(value, &out);
}

inline std::optional<Json::Value> load_json(std::istream &in) {
    Json::Value root;
    JSONCPP_STRING errs;

    if (const Json::CharReaderBuilder builder; !Json::parseFromStream(builder, in, &root, &errs)) {
        std::cerr << errs << '\n';
        return std::nullopt;
    }

    return root;
}

inline std::optional<QueryPlan> load_query_plan(std::istream &in) {
    const auto json_doc = load_json(in);
    if (!json_doc) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return std::nullopt;
    }

    return QueryPlanSerDes::decode(*json_doc);
}

inline void dump_query_plan(const QueryPlan &query_plan, std::ostream &out) {
    const Json::Value query_plan_encoded = QueryPlanSerDes::encode(query_plan);
    dump_json(query_plan_encoded, out);
}

#endif /* SERDE_H_ */
