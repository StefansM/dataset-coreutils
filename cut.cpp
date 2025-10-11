#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include "options.h"
#include "query.h"
#include "queryplan.h"
#include "serde.h"


class CutOptions final : public Options {
public:
    CutOptions() {
        namespace po = boost::program_options;

        // clang-format off
        description().add_options()
        ("field,f", po::value(&fields_)->composing(), "Include this field in output.");
        // clang-format on
        add_positional_argument("field", {.min_args = 1, .max_args = std::nullopt});
    }

    [[nodiscard]] std::vector<std::string> get_fields() const {
        return fields_;
    }

private:
    std::vector<std::string> fields_;
};


int main(
    const int argc,
    const char *argv[]
) {
    CutOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return 1;
    }

    query_plan->select.emplace(query_plan->select->get_tablename(), options.get_fields());

    return static_cast<int>(dump_or_eval_query_plan(*query_plan));
}
