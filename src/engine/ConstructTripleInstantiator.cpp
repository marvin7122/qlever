// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructDeduplicator.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/Views.h"

namespace qlever::constructExport {

// _____________________________________________________________________________
std::optional<EvaluatedTerm> instantiateTerm(
    const PreprocessedTerm& term, const BatchEvaluationResult& batchResult,
    size_t rowIdxInBatch, size_t rowIdxTotal) {
  return std::visit(
      [&](const auto& t) -> std::optional<EvaluatedTerm> {
        using T = std::decay_t<decltype(t)>;

        if constexpr (std::is_same_v<T, PrecomputedConstant>) {
          return t.evaluatedTerm_;
        } else if constexpr (std::is_same_v<T, PrecomputedVariable>) {
          return batchResult.getVariable(t.columnIndex_, rowIdxInBatch);
        } else if constexpr (std::is_same_v<T, PrecomputedBlankNode>) {
          return std::make_shared<const EvaluatedTermData>(EvaluatedTermData{
              absl::StrCat(t.prefix_, rowIdxTotal, t.suffix_), nullptr});
        } else {
          static_assert(ad_utility::alwaysFalse<T>, "Unhandled variant type");
        }
      },
      term);
}

namespace {
// Instantiates one template triple for one result row, or returns
// `nullopt` if a term is undefined or the triple is a duplicate under
// `deduplication`.
std::optional<EvaluatedTriple> tryInstantiateTriple(
    const PreprocessedTriple& triple, const BatchEvaluationResult& batchResult,
    size_t rowInBatch, size_t blankNodeRowId, size_t tripleIdx,
    const PreprocessedConstructTemplate& tmpl,
    const std::optional<DeduplicationParams>& deduplication) {
  auto instantiate = [&triple, &batchResult, rowInBatch,
                      blankNodeRowId](size_t pos) {
    return instantiateTerm(triple.at(pos), batchResult, rowInBatch,
                           blankNodeRowId);
  };
  auto subject = instantiate(0);
  auto predicate = instantiate(1);
  auto object = instantiate(2);
  if (!subject || !predicate || !object) {
    return std::nullopt;
  }
  if (deduplication) {
    const size_t rowIdxInIdTable =
        deduplication.value().ctx_.get().firstRow_ + rowInBatch;
    if (!deduplication.value().deduplicator_.get().isNew(
            tripleIdx, rowIdxInIdTable, tmpl, deduplication.value().ctx_)) {
      return std::nullopt;
    }
  }
  return EvaluatedTriple{*subject, *predicate, *object};
}
}  // namespace

// _____________________________________________________________________________
std::vector<EvaluatedTriple> instantiateBatch(
    const PreprocessedConstructTemplate& tmpl,
    const BatchEvaluationResult& batchResult, size_t batchOffset,
    std::optional<DeduplicationParams> deduplicationParams) {
  std::vector<EvaluatedTriple> triples;
  triples.reserve(batchResult.numRows_ * tmpl.preprocessedTriples_.size());

  for (const size_t rowInBatch :
       ad_utility::integerRange(batchResult.numRows_)) {
    const size_t blankNodeRowId = batchOffset + rowInBatch;
    for (auto&& [tripleIdx, triple] :
         ::ranges::views::enumerate(tmpl.preprocessedTriples_)) {
      if (auto instantiated = tryInstantiateTriple(
              triple, batchResult, rowInBatch, blankNodeRowId,
              static_cast<size_t>(tripleIdx), tmpl, deduplicationParams)) {
        triples.push_back(std::move(*instantiated));
      }
    }
  }
  return triples;
}

namespace {
// Return true iff `formatTerm(term, includeDataType)` is
// `term.rdfTermString()` itself (no `"..."^^<datatype>` annotation).
bool isInFinalForm(const EvaluatedTermData& term, bool includeDataType) {
  const char* type = term.rdfTermDataType();
  if (type == nullptr) {
    // IRI, blank node, or vocab-indexed literal: already in final form.
    return true;
  }
  const auto* i = static_cast<const char*>(XSD_INT_TYPE);
  const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
  const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);

  // Note: XSD_DOUBLE_TYPE values (for example "NaN", "INF", "-INF") always
  // include the datatype.
  return !includeDataType && (type == i || type == d ||
                              (type == b && term.rdfTermString().length() > 1));
}

// Append `formatTerm(term, includeDataType)` to `out`. If `escapeLiteral` is
// true and `term` is a literal (either an encoded literal, which is always a
// literal, or a term whose string starts with `"`), the term is appended in
// the form of `RdfEscaping::validRDFLiteralFromNormalized`.
void appendFormattedTerm(std::string& out, const EvaluatedTermData& term,
                         bool includeDataType, bool escapeLiteral) {
  if (!isInFinalForm(term, includeDataType)) {
    // The `"..."^^<datatype>` form, which always is a literal.
    std::string formatted = formatTerm(term, includeDataType);
    out += escapeLiteral ? RdfEscaping::validRDFLiteralFromNormalized(formatted)
                         : formatted;
    return;
  }
  const std::string_view str = term.rdfTermString();
  if (escapeLiteral && ql::starts_with(str, '"')) {
    out += RdfEscaping::validRDFLiteralFromNormalized(str);
  } else {
    out += str;
  }
}
}  // namespace

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType) {
  const std::string_view str = term.rdfTermString();
  if (isInFinalForm(term, includeDataType)) {
    return std::string{str};
  }
  return absl::StrCat("\"", str, "\"^^<", term.rdfTermDataType(), ">");
}

// _____________________________________________________________________________
void appendFormattedTriple(std::string& out,
                           const EvaluatedTriple& evaluatedTriple,
                           const ad_utility::MediaType& format) {
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const auto& [subject, predicate, object] = evaluatedTriple;

  const bool includeDataType = (format == ntriples);

  if (format == turtle || format == ntriples) {
    // Only escape the object, and only if it is a literal. The terms are
    // appended without an intermediate string unless they need formatting.
    appendFormattedTerm(out, *subject, includeDataType, false);
    out += ' ';
    appendFormattedTerm(out, *predicate, includeDataType, false);
    out += ' ';
    appendFormattedTerm(out, *object, includeDataType, true);
    out += " .\n";
    return;
  }

  std::string s = formatTerm(*subject, includeDataType);
  std::string p = formatTerm(*predicate, includeDataType);
  std::string o = formatTerm(*object, includeDataType);
  if (format == csv) {
    absl::StrAppend(&out, RdfEscaping::escapeForCsv(std::move(s)), ",",
                    RdfEscaping::escapeForCsv(std::move(p)), ",",
                    RdfEscaping::escapeForCsv(std::move(o)), "\n");
  } else {
    AD_CORRECTNESS_CHECK(format == tsv);
    absl::StrAppend(&out, RdfEscaping::escapeForTsv(std::move(s)), "\t",
                    RdfEscaping::escapeForTsv(std::move(p)), "\t",
                    RdfEscaping::escapeForTsv(std::move(o)), "\n");
  }
}

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format) {
  std::string result;
  appendFormattedTriple(result, evaluatedTriple, format);
  return result;
}

// _____________________________________________________________________________
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType) {
  const auto& [subject, predicate, object] = evaluatedTriple;

  std::string s = formatTerm(*subject, includeDataType);
  std::string p = formatTerm(*predicate, includeDataType);
  std::string o = formatTerm(*object, includeDataType);

  return StringTriple{std::move(s), std::move(p), std::move(o)};
}
}  // namespace qlever::constructExport
