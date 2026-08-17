// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
// 2022 - 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/ExportIds.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <charconv>
#include <cmath>
#include <cstdio>

#include "backports/StartsWithAndEndsWith.h"
#include "global/Constants.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "util/Exception.h"

namespace ql::exportIds {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using Iri = ad_utility::triple_component::Iri;
using IriView = ad_utility::triple_component::IriView;
using Literal = ad_utility::triple_component::Literal;

// _____________________________________________________________________________
std::optional<Literal> idToLiteralForEncodedValue(
    Id id, bool onlyReturnLiteralsWithXsdString) {
  if (onlyReturnLiteralsWithXsdString) {
    return std::nullopt;
  }
  auto optionalStringAndType =
      ql::exportIds::idToStringAndTypeForEncodedValue(id);
  if (!optionalStringAndType) {
    return std::nullopt;
  }

  return Literal::literalWithoutQuotes(optionalStringAndType->first);
}

// _____________________________________________________________________________
std::string replaceAnglesByQuotes(std::string iriString) {
  AD_CORRECTNESS_CHECK(ql::starts_with(iriString, '<'));
  AD_CORRECTNESS_CHECK(ql::ends_with(iriString, '>'));
  iriString[0] = '"';
  iriString[iriString.size() - 1] = '"';
  return iriString;
}

// _____________________________________________________________________________
std::optional<Literal> handleIriOrLiteral(
    LiteralOrIri word, bool onlyReturnLiteralsWithXsdString) {
  if (word.isIri()) {
    if (onlyReturnLiteralsWithXsdString) {
      return std::nullopt;
    }
    return Literal::fromStringRepresentation(replaceAnglesByQuotes(
        std::move(word.getIri()).toStringRepresentation()));
  }
  AD_CORRECTNESS_CHECK(word.isLiteral());
  if (onlyReturnLiteralsWithXsdString) {
    if (word.hasDatatype()) {
      return std::nullopt;
    }
    return std::move(word.getLiteral());
  }
  // Note: `removeDatatypeOrLanguageTag` also correctly works if the literal has
  // neither a datatype nor a language tag, hence we don't need an `if` here.
  word.getLiteral().removeDatatypeOrLanguageTag();
  return std::move(word.getLiteral());
}

// _____________________________________________________________________________
std::optional<Literal> idToLiteral(const IndexImpl& index, Id id,
                                   const LocalVocab& localVocab,
                                   bool onlyReturnLiteralsWithXsdString) {
  using enum Datatype;
  auto datatype = id.getDatatype();

  switch (datatype) {
    case WordVocabIndex:
      return getLiteralOrNullopt(getLiteralOrIriFromWordVocabIndex(index, id));
    case EncodedVal:
      return handleIriOrLiteral(encodedIdToLiteralOrIri(id, index),
                                onlyReturnLiteralsWithXsdString);
    case VocabIndex:
    case LocalVocabIndex:
      return handleIriOrLiteral(
          getLiteralOrIriFromVocabIndex(index, id, localVocab),
          onlyReturnLiteralsWithXsdString);
    case TextRecordIndex:
      return getLiteralOrNullopt(getLiteralOrIriFromTextRecordIndex(index, id));
    default:
      return idToLiteralForEncodedValue(id, onlyReturnLiteralsWithXsdString);
  }
}

// _____________________________________________________________________________
std::optional<Literal> getLiteralOrNullopt(
    std::optional<LiteralOrIri> litOrIri) {
  if (litOrIri.has_value() && litOrIri.value().isLiteral()) {
    return std::move(litOrIri.value().getLiteral());
  }
  return std::nullopt;
};

// _____________________________________________________________________________
std::optional<LiteralOrIri> idToLiteralOrIriForEncodedValue(Id id) {
  // TODO<RobinTF> This returns a `nullptr` for the datatype when the `id`
  // represents a `BlankNode` or an `EncodedVal`. The latter case is typically
  // no problem, because the only caller of this function already properly
  // handles this case. The former case is also fine, because `BlankNode`s are
  // neither IRIs nor literals, so returning `std::nullopt` is the correct
  // behavior. However, this is somewhat fragile and should be kept in mind if
  // this function is used in other contexts.
  auto [literal, type] =
      ql::exportIds::idToStringAndTypeForEncodedValue(id).value_or(
          std::make_pair(std::string{}, nullptr));
  if (type == nullptr) {
    return std::nullopt;
  }
  auto lit =
      ad_utility::triple_component::Literal::literalWithoutQuotes(literal);
  lit.addDatatype(
      ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(type));
  return LiteralOrIri{std::move(lit)};
}

// _____________________________________________________________________________
LiteralOrIri getLiteralOrIriFromWordVocabIndex(const IndexImpl& index, Id id) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithoutQuotes(
          index.indexToString(id.getWordVocabIndex()))};
};

// _____________________________________________________________________________
std::optional<LiteralOrIri> getLiteralOrIriFromTextRecordIndex(
    const IndexImpl& index, Id id) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithoutQuotes(
          index.getTextExcerpt(id.getTextRecordIndex()))};
};

// _____________________________________________________________________________
std::optional<LiteralOrIri> idToLiteralOrIri(const IndexImpl& index, Id id,
                                             const LocalVocab& localVocab,
                                             bool skipEncodedValues) {
  using enum Datatype;
  switch (id.getDatatype()) {
    case WordVocabIndex:
      return getLiteralOrIriFromWordVocabIndex(index, id);
    case VocabIndex:
    case LocalVocabIndex:
    case EncodedVal:
      return ql::exportIds::getLiteralOrIriFromVocabIndex(index, id,
                                                          localVocab);
    case TextRecordIndex:
      return getLiteralOrIriFromTextRecordIndex(index, id);
    default:
      if (skipEncodedValues) {
        return std::nullopt;
      }
      return idToLiteralOrIriForEncodedValue(id);
  }
}

// _____________________________________________________________________________
template <typename IriType>
std::optional<std::string_view> blankNodeIriToString(const IriType& iri) {
  std::string_view representation = iri.toStringRepresentation();
  if (ql::starts_with(representation, QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX)) {
    representation.remove_prefix(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX.size());
    representation.remove_suffix(1);
    AD_CORRECTNESS_CHECK(ql::starts_with(representation, "_:"));
    return representation;
  }
  return std::nullopt;
}

template std::optional<std::string_view> blankNodeIriToString<Iri>(const Iri&);
template std::optional<std::string_view> blankNodeIriToString<IriView>(
    const IriView&);

// _____________________________________________________________________________
LiteralOrIri getLiteralOrIriFromVocabIndex(const IndexImpl& index, Id id,
                                           const LocalVocab& localVocab) {
  switch (id.getDatatype()) {
    case Datatype::LocalVocabIndex:
      return localVocab.getWord(id.getLocalVocabIndex()).asLiteralOrIri();
    case Datatype::VocabIndex: {
      auto getEntity = [&index, id]() {
        return index.indexToString(id.getVocabIndex());
      };
      // The type of entity might be `string_view` (If the vocabulary is stored
      // uncompressed in RAM) or `string` (if it is on-disk, or compressed or
      // both). The following code works and is efficient in all cases. In
      // particular, the `std::string` constructor is compiled out because of
      // RVO if `getEntity()` already returns a `string`.
      static_assert(ad_utility::SameAsAny<decltype(getEntity()), std::string,
                                          std::string_view>);
      return LiteralOrIri::fromStringRepresentation(std::string(getEntity()));
    }
    case Datatype::EncodedVal:
      return encodedIdToLiteralOrIri(id, index);
    default:
      AD_FAIL();
  }
}

namespace {
void appendSnprintf(std::string& out, const char* fmt, double d) {
  char buf[64];
  const int n = std::snprintf(buf, sizeof(buf), fmt, d);
  AD_CORRECTNESS_CHECK(n > 0 && static_cast<size_t>(n) < sizeof(buf));
  out.append(buf, static_cast<size_t>(n));
}
}  // namespace

// _____________________________________________________________________________
AppendedId appendIdToStringForEncodedValue(std::string& out, Id id) {
  using enum Datatype;
  switch (id.getDatatype()) {
    case Undefined:
      return {};
    case Double: {
      double d = id.getDouble();
      if (!std::isfinite(d)) {
        if (std::isnan(d)) {
          out.append("NaN");
        } else {
          AD_CORRECTNESS_CHECK(std::isinf(d));
          out.append(d > 0 ? "INF" : "-INF");
        }
        return {true, XSD_DOUBLE_TYPE};
      }
      double dIntPart;
      if (std::modf(d, &dIntPart) == 0.0) {
        appendSnprintf(out, "%.1f", d);
      } else {
        const size_t begin = out.size();
        appendSnprintf(out, "%.13g", d);
        std::string_view written{out.data() + begin, out.size() - begin};
        if (written.find_last_of(".e") == std::string_view::npos) {
          out += ".0";
        }
      }
      return {true, XSD_DECIMAL_TYPE};
    }
    case Bool:
      out.append(id.getBoolLiteral());
      return {true, XSD_BOOLEAN_TYPE};
    case Int: {
      char buf[32];
      auto [end, err] = std::to_chars(buf, buf + sizeof(buf), id.getInt());
      AD_CORRECTNESS_CHECK(err == std::errc{});
      out.append(buf, static_cast<size_t>(end - buf));
      return {true, XSD_INT_TYPE};
    }
    case Date: {
      auto [s, type] = id.getDate().toStringAndType();
      out.append(s);
      return {true, type};
    }
    case GeoPoint: {
      auto [s, type] = id.getGeoPoint().toStringAndType();
      out.append(s);
      return {true, type};
    }
    case BlankNodeIndex:
      out.append("_:bn");
      {
        char buf[24];
        auto [end, err] =
            std::to_chars(buf, buf + sizeof(buf), id.getBlankNodeIndex().get());
        AD_CORRECTNESS_CHECK(err == std::errc{});
        out.append(buf, static_cast<size_t>(end - buf));
      }
      return {true, nullptr};
    case EncodedVal:
      absl::StrAppend(&out, "encodedId: ", id.getBits());
      return {true, nullptr};
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
std::optional<std::pair<std::string, const char*>>
idToStringAndTypeForEncodedValue(Id id) {
  std::string out;
  auto appended = appendIdToStringForEncodedValue(out, id);
  if (!appended.written) {
    return std::nullopt;
  }
  return std::pair{std::move(out), appended.xsdType};
}

// _____________________________________________________________________________
LiteralOrIri encodedIdToLiteralOrIri(Id id, const IndexImpl& index) {
  const auto& mgr = index.encodedIriManager();
  return LiteralOrIri::fromStringRepresentation(mgr.toString(id));
}

// _____________________________________________________________________________
PartitionedIdPositions partitionIdPositions(ql::span<const Id> ids) {
  PartitionedIdPositions positions;
  positions.vocabIndexIndices_.reserve(ids.size());
  positions.nonVocabIndexIndices_.reserve(ids.size());
  ql::ranges::partition_copy(
      ql::views::iota(size_t{0}, ids.size()),
      std::back_inserter(positions.vocabIndexIndices_),
      std::back_inserter(positions.nonVocabIndexIndices_), [&ids](size_t i) {
        return ids[i].getDatatype() == Datatype::VocabIndex;
      });
  return positions;
}

}  // namespace ql::exportIds
