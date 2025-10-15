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
        ("dataset,d", po::value(&dataset_), "Dataset location")
        ("alias,a", po::value(&alias_), "Dataset location")
        ;
        // clang-format on
        add_positional_argument("dataset", {.min_args = 1, .max_args = 1});
    }

    [[nodiscard]] std::string get_dataset() const {
        return dataset_;
    }

    [[nodiscard ]] std::optional<std::string> get_alias() const {
        return alias_ ? std::make_optional(*alias_) : std::nullopt;
    }

private:
    std::string dataset_;
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
    query_plan.select = SelectFragment(options.get_dataset(), {"*"}, options.get_alias());

    overall_plan.add_plan(query_plan);

    return static_cast<int>(dump_or_eval_query_plan(overall_plan));
}
