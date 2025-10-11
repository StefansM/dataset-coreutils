#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "options.h"
#include "queryplan.h"
#include "serde.h"


class SqlOptions final : public Options {
public:
    SqlOptions() {
        namespace po = boost::program_options;

        description().add_options()
        ("sql,s", po::value(&sql_), "SQL query to execute.");
        add_positional_argument("sql", {.min_args = 1, .max_args = 1});
    }

    [[ nodiscard]] std::string get_sql() const { return sql_; }

private:
    std::string sql_;
};


int main(const int argc, const char *argv[]) {
    SqlOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    QueryPlan query_plan {};
    query_plan.sql.emplace(options.get_sql());

    return static_cast<int>(dump_or_eval_query_plan(query_plan));
}
