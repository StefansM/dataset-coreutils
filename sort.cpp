#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include "options.h"
#include "queryplan.h"
#include "serde.h"


class SortOptions final : public Options {
public:
    SortOptions() {
        namespace po = boost::program_options;

        description().add_options()("field,f", po::value(&fields_)->composing(), "Field on which to sort.")(
                "reverse,r", po::bool_switch(&reversed_), "Descending sort.");
        add_positional_argument("field", {.min_args = 1, .max_args = std::nullopt});
    }

    bool parse(const int argc, const char *argv[]) override { // NOLINT(*-avoid-c-arrays)
        if (const bool parent_result = Options::parse(argc, argv); !parent_result) {
            return parent_result;
        }

        return true;
    }

    [[nodiscard]] std::vector<std::string> get_fields() const { return fields_; }
    [[nodiscard]] bool reversed() const { return reversed_; }

private:
    std::vector<std::string> fields_;
    bool reversed_ = false;
};


int main(const int argc, const char *argv[]) {
    SortOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return 1;
    }

    query_plan->order.emplace(options.get_fields(), options.reversed());

    return static_cast<int>(dump_or_eval_query_plan(*query_plan));
}
