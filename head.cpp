#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"


class HeadOptions : public Options {
public:
    HeadOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("lines,n", po::value(&lines_)->default_value(10), "Number of results to include.")
        ;
        add_positional_argument("lines", 0, 1);
    }

    virtual bool parse(int argc, char **argv) override {
        bool parent_result = Options::parse(argc, argv);
        if (!parent_result) {
            return parent_result;
        }

        return true;
    }

    std::uint32_t get_lines() const { return lines_; }

private:
    std::uint32_t lines_;
};


int main(int argc, char **argv) {
    HeadOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    query_plan->limit.emplace(options.get_lines());

    dump_query_plan(*query_plan, std::cout);
    return 0;
}
