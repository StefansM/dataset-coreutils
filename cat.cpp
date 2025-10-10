#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "options.h"
#include "query.h"
#include "queryplan.h"
#include "serde.h"


class CatOptions final : public Options {
public:
    CatOptions() {
        namespace po = boost::program_options;

        description().add_options()("dataset,d", po::value(&dataset_), "Dataset location");
        add_positional_argument("dataset", {.min_args = 1, .max_args = 1});
    }

    [[nodiscard]] std::string get_dataset() const { return dataset_; }

private:
    std::string dataset_;
};


int main(const int argc, const char *argv[]) {
    CatOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    QueryPlan query_plan;
    query_plan.select = SelectFragment(options.get_dataset(), {"*"});

    dump_query_plan(query_plan, std::cout);
    return 0;
}
