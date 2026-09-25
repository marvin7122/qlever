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

#include "./util/GTestHelpers.h"
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
  static_cast<void>(std::move(collector.formatter_).finalize());
  EXPECT_EQ(collector.output_, "\"Title with \\\"quotes\\\"\"");
}

// A fully-qualified encoded literal in CSV output is escaped exactly like
// `RdfEscaping::escapeForCsv` applied to the whole term in `formatTriple`.
// A double-typed literal always takes the fully-qualified form (unlike ints,
// which use the short form in CSV, exactly as in `formatTerm`).
TEST(FastExportStreamFormatterTest, CsvFullyQualifiedLiteralMatchesBaseline) {
  CollectingFormatter collector;
  EvaluatedTermData term{"NaN", XSD_DOUBLE_TYPE};
  collector.formatter_.writeTerm(term, ExportFormat::Csv);
  static_cast<void>(std::move(collector.formatter_).finalize());
  const std::string expected = RdfEscaping::escapeForCsv(
      absl::StrCat("\"NaN\"^^<", XSD_DOUBLE_TYPE, ">"));
  EXPECT_EQ(collector.output_, expected);
  EXPECT_EQ(collector.output_,
            "\"\"\"NaN\"\"^^<http://www.w3.org/2001/"
            "XMLSchema#double>\"");
}

// `writeRow` only supports tabular formats: it throws
// `ad_utility::Exception` for Turtle and N-Triples, while CSV and TSV rows
// use the right delimiter and escaping.
TEST(FastExportStreamFormatterTest, WriteRowRejectsNonTabularFormat) {
  CollectingFormatter collector;
  const std::array<std::string_view, 2> cells{"a,b", "c"};
  AD_EXPECT_THROW_WITH_MESSAGE(
      collector.formatter_.writeRow(ExportFormat::Turtle, cells),
      ::testing::HasSubstr("format == ExportFormat::Csv"));
}

TEST(FastExportStreamFormatterTest, WriteRowCsvAndTsv) {
  CollectingFormatter csvCollector;
  const std::array<std::string_view, 2> cells{"a,b", "c"};
  csvCollector.formatter_.writeRow(ExportFormat::Csv, cells);
  static_cast<void>(std::move(csvCollector.formatter_).finalize());
  EXPECT_EQ(csvCollector.output_, "\"a,b\",c\n");

  CollectingFormatter tsvCollector;
  tsvCollector.formatter_.writeRow(ExportFormat::Tsv, cells);
  static_cast<void>(std::move(tsvCollector.formatter_).finalize());
  EXPECT_EQ(tsvCollector.output_, "a,b\tc\n");
}

// `ensureAvailable` throws, so the write functions must not be `noexcept`:
// an exception escaping a `noexcept` function calls `std::terminate`.
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
  AD_EXPECT_THROW_WITH_MESSAGE(formatter.writeRaw("cdef"),
                               ::testing::HasSubstr("buffer overflow"));
}

}  // namespace
