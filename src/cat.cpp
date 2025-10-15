#include <string>

#include <boost/program_options.hpp>
#include <boost/optional.hpp>

#include "options.h"
#include "query.h"
#include "queryplan.h"
#include "serde.h"


class CatOptions final : public Options {
public:
    CatOptions() {
        namespace po = boost::program_options;

        // clang-format off
        description().add_options()
        ("dataset,d", po::value(&datasets_)->composing(), "Dataset location.")
        ("alias,a", po::value(&alias_), "Alias used for this dataset.")
        ;
        // clang-format on
        add_positional_argument("dataset", {.min_args = 1, .max_args = std::nullopt});
    }

    [[nodiscard]] std::vector<std::string> get_datasets() const {
        return datasets_;
    }

    [[nodiscard ]] std::optional<std::string> get_alias() const {
        return alias_ ? std::make_optional(*alias_) : std::nullopt;
    }

private:
    std::vector<std::string> datasets_;
    boost::optional<std::string> alias_;
};

int main(
    const int argc,
    const char *argv[]
) {
    CatOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    OverallQueryPlan overall_plan;
    if (isatty(fileno(stdin)) != 1) {
        overall_plan = load_query_plan(std::cin).value_or(OverallQueryPlan{});
    }

    QueryPlan query_plan;
    query_plan.select = SelectFragment(options.get_datasets(), {"*"}, options.get_alias());

    overall_plan.add_plan(query_plan);

    return static_cast<int>(dump_or_eval_query_plan(overall_plan));
}
