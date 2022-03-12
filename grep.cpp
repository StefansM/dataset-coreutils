#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"


class GrepOptions : public Options {
public:
    GrepOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("field,f", po::value(&field_), "Field to search.")
            ("value,v", po::value(&value_str_), "Value to search for.")
            ("predicate,p", po::value(&predicate_), "Predicate in the search ('=', 'LIKE', etc).")
            ("integer,i", po::bool_switch(&is_integer_), "Value is an integer column.")
            ("text,t", po::bool_switch(&is_text_), "Value is an text column.")
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

        if (is_integer_ && is_text_) {
            std::cerr << "Only one of 'integer' or 'text' may be specified." << std::endl;
            return false;
        }

        if (is_integer_) {
            // Sensible default operator for ints and strings.
            if (predicate_.empty()) {
                predicate_ = "=";
            }

            try {
                value_ = std::make_unique<QueryParam>(std::stoll(value_str_));
            } catch (const std::exception &e) {
                std::cerr << "Couldn't convert '" << value_str_ << "' to number: " << e.what() << std::endl;
                return false;
            }

        } else if (is_text_) {
            if (predicate_.empty()) {
                predicate_ = "SIMILAR TO";
            }

            value_ = std::make_unique<QueryParam>(value_str_);
        } else {
            if (predicate_.empty()) {
                predicate_ = "LIKE";
            }

            value_ = std::make_unique<QueryParam>(QueryParam::unknown(value_str_));
        }

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

    bool is_integer_ = false;
    bool is_text_ = false;
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
