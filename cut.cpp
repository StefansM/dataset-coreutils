#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"


class CutOptions : public Options {
public:
    CutOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("field,f", po::value(&fields_)->composing(), "Include this field in output.")
        ;
        add_positional_argument("field", 1, -1);
    }

    std::vector<std::string> get_fields() const { return fields_; }

private:
    std::vector<std::string> fields_;
};


int main(int argc, char **argv) {
    CutOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    query_plan->select.emplace(
        query_plan->select->get_tablename(),
        options.get_fields()
    );

    dump_query_plan(*query_plan, std::cout);
    return 0;
}
