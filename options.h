#ifndef OPTIONS_H_
#define OPTIONS_H_

#include <string>
#include <vector>

#include <boost/program_options.hpp>

class Options {
public:
    Options()
        : description_("Options") {

        description_.add_options()
            ("help,h", "Show this help message")
        ;
    }

    virtual ~Options() = default;

    virtual bool parse(int argc, char **argv) {
        namespace po = boost::program_options;

        try {
            po::variables_map var_map;
            po::store(
                po::command_line_parser(argc, argv)
                    .options(description_)
                    .positional(positional_)
                    .run(),
                var_map
            );
            po::notify(var_map);

            if (var_map.count("help")) {
                std::cerr << description_ << std::endl;
                return false;
            }

            for (const auto &[arg_name, min_args] : min_args_) {
                if (var_map.count(arg_name) < min_args) {
                    std::cerr
                        << "Argument '" << arg_name << "' must be supplied at least " << min_args << " times."
                        << std::endl
                        << std::endl
                        << description_
                        << std::endl;
                    return false;
                }
            }
        } catch (const po::error &e) {
            std::cerr << e.what() << std::endl;
            return false;
        }


        return true;
    }

protected:
    void add_positional_argument(std::string arg_name, unsigned int min_args, int max_args) {
        positional_.add(arg_name.c_str(), max_args);
        min_args_[arg_name] = min_args;
    }

    boost::program_options::options_description description_;
    boost::program_options::positional_options_description positional_;

private:
    std::map<std::string, unsigned int> min_args_;
};

#endif /* OPTIONS_H_ */

