#include <iostream>
#include <exception>
#include <string>
#include <vector>
#include <sstream>
#include <cstdint>

#include <getopt.h>

#include <json/json.h>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"

#define PROJECT_NAME "bash-functions"

#include <boost/program_options.hpp>

class CatOptions : public Options {
public:
    CatOptions() {
        namespace po = boost::program_options;

        description_.add_options()
            ("dataset,d", po::value(&dataset_), "Dataset location")
        ;
        add_positional_argument("dataset", 1, 1);
    }

    std::string get_dataset() const { return dataset_; }

private:
    std::string dataset_;
};


int main(int argc, char **argv) {
    CatOptions options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    QueryPlan query_plan;
    query_plan.select = SelectFragment(options.get_dataset(), {"*"});

    Json::Value query_plan_encoded = QueryPlanSerDes().encode(query_plan);
    dump_json(query_plan_encoded, std::cout);
    return 0;
}
