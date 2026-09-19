// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "engine/ConstructTypes.h"
#include "engine/FastExportStreamFormatter.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"

namespace {

using namespace ql::export_formatting;
using namespace qlever::constructExport;

// Collect everything the formatter emits in streaming mode.
struct CollectingFormatter {
  std::string output_;
  FastExportStreamFormatter formatter_;
  CollectingFormatter()
      : formatter_([this](std::string_view chunk) {
          output_.append(chunk.data(), chunk.size());
        }) {}
};

// Turtle output for a plain literal must equal the input bytes: the
// no-special-characters fast path writes the literal through untouched.
TEST(FastExportStreamFormatterTest, TurtlePlainLiteralPassesThrough) {
  CollectingFormatter collector;
  EvaluatedTermData term{"\"Simple Label 5\"@en", nullptr};
  collector.formatter_.writeTerm(term, ExportFormat::Turtle);
  auto summary = std::move(collector.formatter_).finalize();
  EXPECT_EQ(collector.output_, "\"Simple Label 5\"@en");
  EXPECT_EQ(summary.totalTriples_, 0u);
}

// Embedded raw quotes in a normalized literal are escaped once, not twice.
TEST(FastExportStreamFormatterTest, TurtleEmbeddedQuotesEscapedOnce) {
  CollectingFormatter collector;
  EvaluatedTermData term{"\"Title with \"quotes\"\"", nullptr};
  collector.formatter_.writeTerm(term, ExportFormat::Turtle);
  std::move(collector.formatter_).finalize();
  EXPECT_EQ(collector.output_, "\"Title with \\\"quotes\\\"\"");
}

// A fully-qualified encoded literal in CSV output is escaped exactly like
// `RdfEscaping::escapeForCsv` applied to the whole term in `formatTriple`.
TEST(FastExportStreamFormatterTest, CsvFullyQualifiedLiteralMatchesBaseline) {
  CollectingFormatter collector;
  EvaluatedTermData term{"42", XSD_INT_TYPE};
  collector.formatter_.writeTerm(term, ExportFormat::Csv);
  std::move(collector.formatter_).finalize();
  const std::string expected =
      RdfEscaping::escapeForCsv(absl::StrCat("\"42\"^^<", XSD_INT_TYPE, ">"));
  EXPECT_EQ(collector.output_, expected);
  EXPECT_EQ(collector.output_,
            "\"\"\"42\"\"^^<http://www.w3.org/2001/"
            "XMLSchema#int>\"");
}

// `writeRow` only supports tabular formats; Turtle/N-Triples is a contract
// violation, while CSV and TSV rows use the right delimiter and escaping.
TEST(FastExportStreamFormatterTest, WriteRowRejectsNonTabularFormat) {
  CollectingFormatter collector;
  const std::array<std::string_view, 2> cells{"a,b", "c"};
  EXPECT_THROW(collector.formatter_.writeRow(ExportFormat::Turtle, cells),
               ad_utility::Exception);
}

TEST(FastExportStreamFormatterTest, WriteRowCsvAndTsv) {
  CollectingFormatter csvCollector;
  const std::array<std::string_view, 2> cells{"a,b", "c"};
  csvCollector.formatter_.writeRow(ExportFormat::Csv, cells);
  std::move(csvCollector.formatter_).finalize();
  EXPECT_EQ(csvCollector.output_, "\"a,b\",c\n");

  CollectingFormatter tsvCollector;
  tsvCollector.formatter_.writeRow(ExportFormat::Tsv, cells);
  std::move(tsvCollector.formatter_).finalize();
  EXPECT_EQ(tsvCollector.output_, "a,b\tc\n");
}

// The throwing `ensureAvailable` makes `noexcept` on the write functions
// wrong (an exception in a `noexcept` function calls `std::terminate`).
TEST(FastExportStreamFormatterTest, WriteFunctionsAreNotNoexcept) {
  static_assert(
      !noexcept(std::declval<FastExportStreamFormatter&>().writeChar('x')));
  static_assert(!noexcept(
      std::declval<FastExportStreamFormatter&>().writeRaw(std::string_view{})));
  static_assert(
      !noexcept(std::declval<FastExportStreamFormatter&>().writeInteger(42)));
}

// Fixed-span overflow throws instead of terminating or overwriting memory.
TEST(FastExportStreamFormatterTest, FixedSpanOverflowThrows) {
  std::array<char, 4> buffer{};
  FastExportStreamFormatter formatter{
      ql::span<char>{buffer.data(), buffer.size()}};
  formatter.writeRaw("ab");
  EXPECT_THROW(formatter.writeRaw("cdef"), ad_utility::Exception);
}

}  // namespace
