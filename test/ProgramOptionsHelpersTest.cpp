// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "util/ProgramOptionsHelpers.h"

namespace po = boost::program_options;

namespace {
size_t parseNumSimultaneousQueries(const std::vector<std::string>& arguments) {
  ad_utility::Positive count = 1;
  po::options_description options;
  options.add_options()(
      "num-simultaneous-queries,j",
      po::value<ad_utility::Positive>(&count)->default_value(1));
  po::variables_map variables;
  po::store(po::command_line_parser(arguments).options(options).run(),
            variables);
  po::notify(variables);
  return count;
}
}  // namespace

TEST(ProgramOptionsHelpersTest, RejectsZeroConcurrentQueries) {
  EXPECT_THROW(parseNumSimultaneousQueries({"--num-simultaneous-queries=0"}),
               po::validation_error);
}

TEST(ProgramOptionsHelpersTest, DefaultAndValidConcurrentQueries) {
  EXPECT_EQ(parseNumSimultaneousQueries({}), 1u);
  for (const auto& value :
       {std::string{"1"}, std::string{"42"},
        std::to_string(std::numeric_limits<size_t>::max())}) {
    SCOPED_TRACE(value);
    auto expected = boost::lexical_cast<size_t>(value);
    EXPECT_EQ(
        parseNumSimultaneousQueries({"--num-simultaneous-queries", value}),
        expected);
    EXPECT_EQ(
        parseNumSimultaneousQueries({"--num-simultaneous-queries=" + value}),
        expected);
    EXPECT_EQ(parseNumSimultaneousQueries({"-j", value}), expected);
    EXPECT_EQ(parseNumSimultaneousQueries({"-j" + value}), expected);
  }
  std::ostringstream stream;
  std::ostream& output = stream;
  output << ad_utility::Positive{1} << ' ' << ad_utility::NonNegative{0};
  EXPECT_EQ(stream.str(), "1 0");
}

TEST(ProgramOptionsHelpersTest, ZeroDiagnosticNamesOptionAndValue) {
  for (const std::string value : {"0", "00"}) {
    SCOPED_TRACE(value);
    for (const auto& arguments :
         {std::vector<std::string>{"--num-simultaneous-queries=" + value},
          std::vector<std::string>{"-j", value}}) {
      try {
        static_cast<void>(parseNumSimultaneousQueries(arguments));
        FAIL() << "Zero must be rejected";
      } catch (const po::validation_error& error) {
        EXPECT_EQ(error.kind(), po::validation_error::invalid_option_value);
        EXPECT_EQ(error.get_option_name(), "--num-simultaneous-queries");
        const std::string message = error.what();
        EXPECT_NE(message.find("num-simultaneous-queries"), std::string::npos);
        EXPECT_NE(message.find("'" + value + "'"), std::string::npos)
            << message;
      }
    }
  }
}

TEST(ProgramOptionsHelpersTest, RejectsNegativeAndNegativeZeroValues) {
  for (const std::string value : {"-0", "-1", "-42"}) {
    SCOPED_TRACE(value);
    EXPECT_THROW(
        parseNumSimultaneousQueries({"--num-simultaneous-queries=" + value}),
        std::runtime_error);
    EXPECT_THROW(parseNumSimultaneousQueries({"-j", value}),
                 std::runtime_error);
  }
}

TEST(ProgramOptionsHelpersTest, RejectsMalformedAndOverflowValues) {
  for (const auto& value :
       {std::string{}, std::string{"abc"}, std::string{"1.5"},
        std::string{"1x"},
        std::to_string(std::numeric_limits<size_t>::max()) + "0"}) {
    SCOPED_TRACE(value);
    EXPECT_THROW(
        parseNumSimultaneousQueries({"--num-simultaneous-queries=" + value}),
        std::exception);
  }
}

TEST(ProgramOptionsHelpersTest, RejectsDuplicateOccurrence) {
  EXPECT_THROW(
      parseNumSimultaneousQueries({"--num-simultaneous-queries=1", "-j", "2"}),
      po::multiple_occurrences);
}

TEST(ProgramOptionsHelpersTest, NonNegativeStillAcceptsZero) {
  ad_utility::NonNegative count = 1;
  po::options_description options;
  options.add_options()("non-negative",
                        po::value<ad_utility::NonNegative>(&count));
  po::variables_map variables;
  po::store(
      po::command_line_parser(std::vector<std::string>{"--non-negative=0"})
          .options(options)
          .run(),
      variables);
  po::notify(variables);
  EXPECT_EQ(static_cast<size_t>(count), 0u);
}
