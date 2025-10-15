#include <string>

#include <boost/program_options.hpp>

#include "options.h"
#include "queryplan.h"
#include "serde.h"


class SqlOptions final : public Options {
public:
    SqlOptions() {
        namespace po = boost::program_options;

        // clang-format off
        description().add_options()
        ("sql,s", po::value(&sql_), "SQL query to execute.");
        // clang-format on
        add_positional_argument("sql", {.min_args = 1, .max_args = 1});
    }

    [[ nodiscard]] std::string get_sql() const {
        return sql_;
    }

private:
    std::string sql_;
};


int main(
    const int argc,
    const char *argv[]
) {
    SqlOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    OverallQueryPlan overall_plan;
    if (isatty(fileno(stdin)) != 1) {
        overall_plan = load_query_plan(std::cin).value_or(OverallQueryPlan{});
    }
    QueryPlan query_plan{};
    query_plan.sql.emplace(options.get_sql());

    overall_plan.add_plan(query_plan);

    return static_cast<int>(dump_or_eval_query_plan(overall_plan));
}
