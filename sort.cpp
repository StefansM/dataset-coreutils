#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"


class SortOptions : public Options {
public:
    SortOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("field,f", po::value(&fields_)->composing(), "Field on which to sort.")
            ("reverse,r", po::bool_switch(&reversed_), "Descending sort.")
        ;
        add_positional_argument("field", 1, -1);
    }

    virtual bool parse(int argc, char **argv) override {
        bool parent_result = Options::parse(argc, argv);
        if (!parent_result) {
            return parent_result;
        }

        return true;
    }

    std::vector<std::string> get_fields() const { return fields_; }
    bool reversed() const { return reversed_; }

private:
    std::vector<std::string> fields_;
    bool reversed_ = false;
};


int main(int argc, char **argv) {
    SortOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    query_plan->order.emplace(options.get_fields(), options.reversed());

    dump_query_plan(*query_plan, std::cout);
    return 0;
}
