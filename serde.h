#ifndef SERDE_H_
#define SERDE_H_

#include <json/json.h>

class QueryParamSerDes {

public:
    Json::Value encode(const QueryParam &fragment) {
        Json::Value value;
        switch (fragment.type) {
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

    QueryParam decode(const Json::Value &fragment) {
        auto type = fragment["type"].asString();
        auto value = fragment["value"];
        if (type == "NUMERIC")
            return QueryParam(value.asInt64());
        else if (type == "TEXT")
            return QueryParam(value.asString());
        else
            return QueryParam::unknown(value.asString());
    }
};

class SelectSerDes {
public:
    Json::Value encode(const SelectFragment &fragment) {
        Json::Value value;
        value["tablename"] = fragment.get_tablename();

        int i = 0;
        for (const auto &col : fragment.get_columns()) {
            value["columns"][i++] = col;
        }

        return value;
    }

    SelectFragment decode(const Json::Value &fragment) {
        std::vector<std::string> columns;
        for (const auto &col : fragment["columns"]) {
            columns.push_back(col.asString());
        }

        return SelectFragment(fragment["tablename"].asString(), columns);
    }
};

class WhereSerDes {
public:
    Json::Value encode(const WhereFragment &fragment) {
        Json::Value value;
        value["conditions"] = Json::Value();

        int i = 0;
        for (const auto &cond : fragment.get_conditions()) {
            Json::Value json_cond;
            json_cond["column"] = cond.column;
            json_cond["predicate"] = cond.predicate;
            json_cond["value"] = param_serdes.encode(cond.value);

            value["conditions"][i++] = json_cond;
        }

        return value;
    }

    WhereFragment decode(const Json::Value &json) {
        WhereFragment fragment;
        for (const auto &cond : json["conditions"]) {
            auto column = cond["column"].asString();
            auto predicate = cond["predicate"].asString();
            auto value = param_serdes.decode(cond["value"]);

            fragment.add_condition(column, predicate, value);
        }

        return fragment;
    }

private:
    QueryParamSerDes param_serdes;
};

class LimitSerDes {
public:
    Json::Value encode(const LimitFragment &fragment) {
        Json::Value value;
        value["limit"] = fragment.get_limit();
        return value;
    }

    LimitFragment decode(const Json::Value &json) {
        return LimitFragment(json["limit"].asUInt());
    }
};

class QueryPlanSerDes {
public:
    Json::Value encode(const QueryPlan &query_plan) {
        Json::Value root;
        root["select"] = Json::Value::null;
        root["where"] = Json::Value::null;
        root["limit"] = Json::Value::null;

        if (query_plan.select)
            root["select"] = SelectSerDes().encode(*query_plan.select);

        if (query_plan.where)
            root["where"] = WhereSerDes().encode(*query_plan.where);

        if (query_plan.limit)
            root["limit"] = LimitSerDes().encode(*query_plan.limit);

        return root;
    }

    QueryPlan decode(const Json::Value &root) {
        QueryPlan query_plan;

        auto select = root["select"];
        if (select != Json::Value::null)
            query_plan.select = SelectSerDes().decode(select);

        auto where = root["where"];
        if (where != Json::Value::null)
            query_plan.where = WhereSerDes().decode(where);

        auto limit = root["limit"];
        if (limit != Json::Value::null)
            query_plan.limit = LimitSerDes().decode(limit);

        return query_plan;
    }
};

void dump_json(const Json::Value &value, std::ostream &out) {
    Json::StreamWriterBuilder builder;
    std::unique_ptr<Json::StreamWriter> json_writer(builder.newStreamWriter());
    json_writer->write(value, &out);
}

std::optional<Json::Value> load_json(std::istream &in) {
    Json::Value root;
    JSONCPP_STRING errs;

    Json::CharReaderBuilder builder;
    if (!Json::parseFromStream(builder, in, &root, &errs)) {
        std::cerr << errs << std::endl;
        return std::nullopt;
    }

    return root;
}

std::optional<QueryPlan> load_query_plan(std::istream &in) {
    auto json_doc = load_json(in);
    if (!json_doc) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return std::nullopt;
    }

    return QueryPlanSerDes().decode(*json_doc);
}

void dump_query_plan(const QueryPlan &query_plan, std::ostream &out) {
    Json::Value query_plan_encoded = QueryPlanSerDes().encode(query_plan);
    dump_json(query_plan_encoded, out);
}

#endif /* SERDE_H_ */

