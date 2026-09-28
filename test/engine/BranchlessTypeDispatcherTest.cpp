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

#include "../util/GTestHelpers.h"
#include "engine/BranchlessTypeDispatcher.h"
#include "global/ValueId.h"
#include "index/LocalVocabEntry.h"

using ql::engine::BranchlessTypeDispatcher;

namespace {

using LookupTable = BranchlessTypeDispatcher::LookupTable;

constexpr std::string_view xsdInteger =
    "\"^^<http://www.w3.org/2001/XMLSchema#integer>";
constexpr std::string_view xsdDouble =
    "\"^^<http://www.w3.org/2001/XMLSchema#double>";
constexpr std::string_view xsdBoolean =
    "\"^^<http://www.w3.org/2001/XMLSchema#boolean>";
constexpr std::string_view xsdDateTime =
    "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>";
constexpr std::string_view wktLiteral =
    "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// Format `id` (with `rawTerm`) with `lut` into a buffer that is large enough
// for every term used in these tests.
std::string format(ValueId id, std::string_view rawTerm,
                   const LookupTable& lut) {
  std::array<char, 256> buffer{};
  const size_t size =
      BranchlessTypeDispatcher::dispatchTermFormat(id, rawTerm, buffer, lut);
  return std::string(buffer.data(), size);
}

// The expected output of a term for each of the three built-in lookup tables.
struct Expected {
  std::string default_;
  std::string turtle_;
  std::string rawVocab_;
};

// Check the output of `id` for all three built-in lookup tables.
void expectFormats(ValueId id, std::string_view rawTerm,
                   const Expected& expected) {
  EXPECT_EQ(format(id, rawTerm, BranchlessTypeDispatcher::defaultLut()),
            expected.default_);
  EXPECT_EQ(format(id, rawTerm, BranchlessTypeDispatcher::turtleLut()),
            expected.turtle_);
  EXPECT_EQ(format(id, rawTerm, BranchlessTypeDispatcher::rawVocabLut()),
            expected.rawVocab_);
}

}  // namespace

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, VocabularyTerms) {
  constexpr std::string_view iri = "http://example.org/resource";
  const Expected asIri{"<http://example.org/resource>",
                       "<http://example.org/resource>", std::string{iri}};
  expectFormats(ValueId::makeFromVocabIndex(VocabIndex::make(42)), iri, asIri);
  expectFormats(
      ValueId::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(7)), iri,
      asIri);
  expectFormats(ValueId::makeFromEncodedVal(3), iri, asIri);
  const auto entry = LocalVocabEntry::fromStringRepresentation("<x>");
  expectFormats(ValueId::makeFromLocalVocabIndex(&entry), iri, asIri);

  constexpr std::string_view text = "Hello World";
  const Expected asText{"\"Hello World\"", "\"Hello World\"",
                        std::string{text}};
  expectFormats(ValueId::makeFromTextRecordIndex(TextRecordIndex::make(10)),
                text, asText);
  expectFormats(ValueId::makeFromWordVocabIndex(WordVocabIndex::make(10)), text,
                asText);
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, Integers) {
  for (int64_t value : {int64_t{0}, int64_t{42}, int64_t{-42}, ValueId::maxInt,
                        ValueId::IntegerType::min()}) {
    const std::string digits = std::to_string(value);
    expectFormats(ValueId::makeFromInt(value), "",
                  {absl::StrCat("\"", digits, xsdInteger), digits, digits});
  }
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, Doubles) {
  // `ValueId` truncates the mantissa of doubles; these values are exactly
  // representable, so their shortest round-trip form is the plain decimal.
  for (const auto& [value, digits] :
       std::vector<std::pair<double, std::string>>{
           {3.5, "3.5"}, {-0.25, "-0.25"}, {1024.0, "1024"}}) {
    expectFormats(ValueId::makeFromDouble(value), "",
                  {absl::StrCat("\"", digits, xsdDouble), digits, digits});
  }
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, Booleans) {
  expectFormats(ValueId::makeFromBool(true), "",
                {absl::StrCat("\"true", xsdBoolean), "true", "true"});
  expectFormats(ValueId::makeFromBool(false), "",
                {absl::StrCat("\"false", xsdBoolean), "false", "false"});
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, BlankNodesDatesGeoPointsAndUndefined) {
  expectFormats(ValueId::makeFromBlankNodeIndex(BlankNodeIndex::make(5)), "",
                {"_:bn5", "_:bn5", "_:bn5"});

  const auto date =
      ValueId::makeFromDate(DateYearOrDuration{Date{2026, 8, 19}});
  const std::string dateString = date.getDate().toStringAndType().first;
  expectFormats(date, "",
                {absl::StrCat("\"", dateString, xsdDateTime),
                 absl::StrCat("\"", dateString, xsdDateTime), dateString});

  const auto point = ValueId::makeFromGeoPoint(GeoPoint{50.0, 8.0});
  const std::string pointString = point.getGeoPoint().toStringAndType().first;
  expectFormats(point, "",
                {absl::StrCat("\"", pointString, wktLiteral),
                 absl::StrCat("\"", pointString, wktLiteral), pointString});

  expectFormats(ValueId::makeUndefined(), "", {"", "", ""});
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, UnmappedDatatypeSlotThrows) {
  // Tag 13 is larger than `Datatype::MaxValue` and has no formatter.
  static_assert(static_cast<uint64_t>(Datatype::MaxValue) < 13);
  const auto id = ValueId::fromBits(uint64_t{13} << ValueId::numDataBits);
  std::array<char, 256> buffer{};
  AD_EXPECT_THROW_WITH_MESSAGE(
      BranchlessTypeDispatcher::dispatchTermFormat(id, "", buffer),
      ::testing::HasSubstr("no formatter"));
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, OutputBufferTooSmallThrows) {
  const auto tooSmall = ::testing::HasSubstr("too small");
  std::array<char, 8> buffer{};
  // The IRI with its delimiters needs 29 bytes.
  AD_EXPECT_THROW_WITH_MESSAGE(
      BranchlessTypeDispatcher::dispatchTermFormat(
          ValueId::makeFromVocabIndex(VocabIndex::make(1)),
          "http://example.org/resource", buffer),
      tooSmall);
  // The value fits, but not its datatype suffix.
  AD_EXPECT_THROW_WITH_MESSAGE(BranchlessTypeDispatcher::dispatchTermFormat(
                                   ValueId::makeFromInt(1), "", buffer),
                               tooSmall);
  AD_EXPECT_THROW_WITH_MESSAGE(BranchlessTypeDispatcher::dispatchTermFormat(
                                   ValueId::makeFromDouble(3.5), "", buffer),
                               tooSmall);
  // A buffer of exactly the output size suffices.
  std::array<char, 4> exactSize{};
  EXPECT_EQ(BranchlessTypeDispatcher::dispatchTermFormat(
                ValueId::makeFromBool(true), "", exactSize,
                BranchlessTypeDispatcher::turtleLut()),
            4u);
  EXPECT_EQ(std::string_view(exactSize.data(), exactSize.size()), "true");
}

// _____________________________________________________________________________
TEST(BranchlessTypeDispatcherTest, BatchFormatting) {
  const std::vector<ValueId> ids{
      ValueId::makeFromVocabIndex(VocabIndex::make(1)),
      ValueId::makeFromInt(100),
      ValueId::makeFromBlankNodeIndex(BlankNodeIndex::make(5)),
      ValueId::makeFromBool(true)};
  const std::vector<std::string_view> rawTerms{"http://example.org/pred", "",
                                               "", ""};
  const auto& lut = BranchlessTypeDispatcher::turtleLut();

  // The batch output is the concatenation of the outputs of the single terms.
  std::string expected;
  for (size_t i = 0; i < ids.size(); ++i) {
    expected += format(ids[i], rawTerms[i], lut);
  }
  EXPECT_EQ(expected, "<http://example.org/pred>100_:bn5true");

  std::array<char, 256> buffer{};
  const size_t size = BranchlessTypeDispatcher::dispatchBatchTermFormat(
      ids, rawTerms, buffer, lut);
  EXPECT_EQ(std::string_view(buffer.data(), size), expected);

  // An empty batch writes nothing, also into an empty buffer.
  EXPECT_EQ(BranchlessTypeDispatcher::dispatchBatchTermFormat({}, {}, {}, lut),
            0u);

  // The sizes of `ids` and `rawTerms` must match.
  EXPECT_ANY_THROW(BranchlessTypeDispatcher::dispatchBatchTermFormat(
      ids, ql::span<const std::string_view>{rawTerms}.subspan(1), buffer, lut));

  // A buffer that holds only part of the batch.
  std::array<char, 30> tooSmall{};
  AD_EXPECT_THROW_WITH_MESSAGE(
      BranchlessTypeDispatcher::dispatchBatchTermFormat(ids, rawTerms, tooSmall,
                                                        lut),
      ::testing::HasSubstr("too small"));
}
