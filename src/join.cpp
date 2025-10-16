#include <algorithm>
#include <string>

#include <boost/program_options.hpp>
#include <boost/optional.hpp>

#include "options.h"
#include "queryplan.h"
#include "serde.h"


class JoinOptions final : public Options {
public:
    JoinOptions() {
        namespace po = boost::program_options;

        // clang-format off
        description().add_options()
        ("table,t", po::value(&table_)->required(), "Table or file to join against.")
        ("alias,a", po::value(&alias_), "Alias to use for the table.")
        ("how,h", po::value(&how_)->default_value("INNER"), "Type of join.")
        ("left,l", po::value(&left_)->composing(), "Column in left hand table.")
        ("predicate,p", po::value(&predicate_)->composing(), "Predicate ('=', '>', etc).")
        ("right,r", po::value(&right_)->composing(), "Column in right hand table.");

        // We don't include the positional arguments in the help message.
        set_help_description(description());

        description().add_options()
        ("join_part", po::value(&positional_args_)->composing(), "Left, predicate and right, in order.");
        // clang-format on

        add_positional_argument("join_part", {.min_args = 0, .max_args = std::nullopt});
    }


    [[nodiscard]] std::vector<JoinCondition> get_conditions() const {
        std::vector<JoinCondition> conditions;
        parse_option_switches(conditions);
        parse_positional_arguments(conditions);
        return conditions;
    }

    [[nodiscard]] std::string get_how() const {
        return how_;
    }

    [[nodiscard]] std::string get_table() const {
        return table_;
    }

    [[nodiscard]] std::optional<std::string> get_alias() const {
        return alias_ ? std::make_optional(*alias_) : std::nullopt;
    }

private:
    std::string table_, how_;
    std::vector<std::string> left_, predicate_, right_;
    std::vector<std::string> positional_args_;
    boost::optional<std::string> alias_;

    void parse_option_switches(
        std::vector<JoinCondition> &conditions
    ) const {
        const auto num_args = std::min({left_.size(), right_.size(), predicate_.size()});
        if (left_.size() != num_args || right_.size() != num_args || predicate_.size() != num_args) {
            std::cerr << "Number of left, right and predicate arguments must be the same. Got " << left_.size() << ", "
                    << right_.size() << " and " << predicate_.size() << ". Using the first " << num_args << "\n";
        }

        for (std::size_t i = 0; i < num_args; ++i) {
            conditions.push_back(JoinCondition{.left = left_[i], .predicate = predicate_[i], .right = right_[i]});
        }
    }

    void parse_positional_arguments(
        std::vector<JoinCondition> &conditions
    ) const {
        const auto num_triplets = positional_args_.size() / 3;
        if (positional_args_.size() % 3 != 0) {
            std::cerr << "Positional arguments must be in groups of three (left, predicate, right). Got " <<
                    positional_args_.size() << " arguments. Using the first " << num_triplets << "\n";
        }
        for (std::size_t i = 0; i < num_triplets; ++i) {
            conditions.push_back(
                JoinCondition{
                    .left = positional_args_[i * 3],
                    .predicate = positional_args_[(i * 3) + 1],
                    .right = positional_args_[(i * 3) + 2]
                }
            );
        }
    }
};


int main(
    const int argc,
    const char *argv[]
) {
    JoinOptions options;
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
    auto &query_plan = overall_query_plan->get_plans().back();

    query_plan.join.emplace(
        JoinFragment{options.get_table(), options.get_how(), options.get_conditions(), options.get_alias()}
    );

    return static_cast<int>(dump_or_eval_query_plan(*overall_query_plan));
}
