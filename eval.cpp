#include <iostream>
#include <string>
#include <vector>

#include <sstream>

#include <duckdb.hpp>

#include "query.h"
#include "queryplan.h"
#include "serde.h"
#include "options.h"

#define PROJECT_NAME "bash-functions"

#include <boost/program_options.hpp>


int main(int argc, char **argv) {
    Options options;
    if (!options.parse(argc, argv)) {
        return 1;
    }

    auto query_plan = load_query_plan(std::cin);
    if (!query_plan) {
        std::cerr << "Unable to parse query plan from standard input." << std::endl;
        return 1;
    }

    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    std::map<std::string, std::string> datatypes;

    auto select = *query_plan->select;
    auto col_query = con.Prepare("DESCRIBE SELECT * FROM " + select.get_tablename()); // WHERE table_name = ? and column_name = ?");
    if (!col_query->success) {
        std::cerr << "Couldn't prepare statement: " << col_query->error << std::endl;
        return 1;
    }

    auto describe_result = col_query->Execute();
    if (!describe_result->success) {
        std::cerr << "Couldn't describe table '" << select.get_tablename() << "': " << describe_result->error << std::endl;
        return 1;
    }
    for (const auto &row : *describe_result) {
        datatypes.emplace(row.GetValue<std::string>(0), row.GetValue<std::string>(1));
        std::cout << row.GetValue<std::string>(0) << ": " << row.GetValue<std::string>(1) << std::endl;
    }

    auto query_str = query_plan->query();
    if (!query_str) {
        return 1;
    }

    std::cout << *query_str << std::endl;
    auto result = con.Query(*query_str);
    if (!result->success) {
        std::cerr << "Error querying DuckDB: " << result->error << std::endl;
        return 2;
    }

    result->Print();
    return 0;
}
