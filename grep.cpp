#include <iostream>
#include <string>
#include <vector>

#include <json/json.h>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"

#define PROJECT_NAME "bash-functions"

#include <boost/program_options.hpp>

class GrepOptions : public Options {
public:
    GrepOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("field,f", po::value(&field_), "Field to search.")
            ("value,v", po::value(&value_str_), "Value to search for.")
            ("predicate,p", po::value(&predicate_)->default_value("LIKE"), "Predicate in the search ('=', 'LIKE', etc).")
        ;
        add_positional_argument("field", 1, 1);
        add_positional_argument("value", 1, 1);
    }

    virtual bool parse(int argc, char **argv) override {
        bool parent_result = Options::parse(argc, argv);
        if (!parent_result) {
            return parent_result;
        }

        if (field_.empty() || value_str_.empty()) {
            std::cerr << "Both 'field' and 'value' option must be supplied." << std::endl;
            return false;
        }

        // TODO: Handle other types
        value_ = std::make_unique<QueryParam>(value_str_);

        return true;
    }

    std::string get_field() const { return field_; }
    std::string get_predicate() const { return predicate_; }
    QueryParam get_value() const { return QueryParam(*value_); }

private:
    std::string field_;
    std::string value_str_;
    std::string predicate_;
    std::unique_ptr<QueryParam> value_;
};


int main(int argc, char **argv) {
    GrepOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    if (!query_plan->where) {
        query_plan->where.emplace();
    }
    query_plan->where->add_condition(options.get_field(), options.get_predicate(), options.get_value());

    dump_query_plan(*query_plan, std::cout);
    return 0;
}
