#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "options.h"
#include "queryplan.h"
#include "serde.h"

constexpr int DEFAULT_NUMBER_OF_LINES = 10;

class HeadOptions final : public Options {
public:
    HeadOptions() {
        namespace po = boost::program_options;

        // clang-format off
        description().add_options()
        ("lines,n", po::value(&lines_)->default_value(DEFAULT_NUMBER_OF_LINES), "Number of results to include.");
        // clang-format on
        add_positional_argument("lines", {.min_args = 0, .max_args = 1});
    }

    bool parse(
        const int argc,
        const char *argv[]
    ) override { // NOLINT(*-avoid-c-arrays)
        if (bool const parent_result = Options::parse(argc, argv); !parent_result) {
            return parent_result;
        }

        return true;
    }

    [[nodiscard]] std::uint32_t get_lines() const {
        return lines_;
    }

private:
    std::uint32_t lines_{};
};


int main(
    const int argc,
    const char *argv[]
) {
    HeadOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto overall_query_plan = load_query_plan(std::cin);
    if (!overall_query_plan) {
        std::cerr << "Unable to parse query plan from standard input.\n";
        return 1;
    }
    if (overall_query_plan->get_plans().empty()) {
        std::cerr << "Empty query plan.\n";
        return 1;
    }

    overall_query_plan->get_plans().back().limit.emplace(options.get_lines());

    return static_cast<int>(dump_or_eval_query_plan(*overall_query_plan));
}
