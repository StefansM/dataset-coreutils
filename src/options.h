#pragma once

#include <iostream>
#include <optional>
#include <string>

#include <boost/program_options.hpp>

struct ArgCount {
    unsigned int min_args = 0;
    std::optional<int> max_args = std::nullopt;
};

class Options {
public:
    Options() :
        description_("Options") {
        description_.add_options()("help,h", "Show this help message");
    }

    Options(
        const Options &
    ) = delete;

    Options &operator=(
        const Options &
    ) = delete;

    Options(
        Options &&
    ) = delete;

    Options &operator=(
        Options &&
    ) = delete;

    virtual ~Options() = default;

    virtual bool parse(
        const int argc,
        const char *argv[] // NOLINT(*-avoid-c-arrays)
    ) {
        namespace po = boost::program_options;

        try {
            po::variables_map var_map;
            po::store(po::command_line_parser(argc, argv).options(description_).positional(positional_).run(), var_map);
            po::notify(var_map);

            if (var_map.contains("help")) {
                const auto &description = help_description_.value_or(description_);
                std::cerr << description << '\n';
                return false;
            }

            for (const auto &[arg_name, min_args]: min_args_) {
                if (var_map.count(arg_name) < min_args) {
                    std::cerr << "Argument '" << arg_name << "' must be supplied at least " << min_args << " times.\n\n"
                            << description_ << '\n';
                    return false;
                }
            }
        } catch (const po::error &e) {
            std::cerr << e.what() << '\n';
            return false;
        }

        return true;
    }

    boost::program_options::options_description &description() {
        return description_;
    }

    boost::program_options::positional_options_description &positional() {
        return positional_;
    }

protected:
    void add_positional_argument(
        const std::string &arg_name,
        const ArgCount &arg_count
    ) {
        const int max_args = arg_count.max_args.value_or(-1);
        const unsigned int min_args = arg_count.min_args;

        positional_.add(arg_name.c_str(), max_args);
        min_args_[arg_name] = min_args;
    }

    void set_help_description(
        boost::program_options::options_description description
    ) {
        help_description_.emplace(description);
    }

private:
    boost::program_options::options_description description_;
    boost::program_options::positional_options_description positional_;

    std::map<std::string, unsigned int> min_args_;
    std::optional<boost::program_options::options_description> help_description_{};
};
