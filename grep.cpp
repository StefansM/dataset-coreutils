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
            ("pattern,p", po::value(&pattern_), "Pattern to search against.")
        ;
        add_positional_argument("pattern", 1, -1);
    }

    virtual bool parse(int argc, char **argv) override {
        bool parent_result = Options::parse(argc, argv);
        if (!parent_result) {
            return parent_result;
        }

        if (field_.empty() || pattern_.empty()) {
            std::cerr << "Both 'field' and 'pattern' option must be supplied." << std::endl;
            return false;
        }

        return true;
    }

    std::string get_field() const { return field_; }
    std::string get_pattern() const { return pattern_; }

private:
    std::string field_;
    std::string pattern_;
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
    query_plan->where->add_condition(options.get_field(), options.get_pattern());

    dump_query_plan(*query_plan, std::cout);
    return 0;
}
