// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

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

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType) {
  if (term.rdfTermDataType_ == nullptr) {
    // IRI, blank node, or vocab-indexed literal: already in final form.
    return term.rdfTermString_;
  }
  const auto* i = static_cast<const char*>(XSD_INT_TYPE);
  const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
  const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);

  // Note: XSD_DOUBLE_TYPE values (for example "NaN", "INF", "-INF") always
  // include the datatype.
  if (!includeDataType &&
      (term.rdfTermDataType_ == i || term.rdfTermDataType_ == d ||
       (term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1))) {
    return term.rdfTermString_;
  }
  return absl::StrCat("\"", term.rdfTermString_, "\"^^<", term.rdfTermDataType_,
                      ">");
}

namespace {
// Returns true iff `formatTerm` (and `appendFormattedTerm`) emit `term` in the
// fully qualified form `"value"^^<datatype>` as opposed to the short form
// without quotation marks.
bool needsDatatypeWrapper(const EvaluatedTermData& term, bool includeDataType) {
  if (term.rdfTermDataType_ == nullptr) {
    return false;
  }
  const auto* i = static_cast<const char*>(XSD_INT_TYPE);
  const auto* d = static_cast<const char*>(XSD_DECIMAL_TYPE);
  const auto* b = static_cast<const char*>(XSD_BOOLEAN_TYPE);
  // Note: XSD_DOUBLE_TYPE values (for example "NaN", "INF", "-INF") always
  // include the datatype.
  return includeDataType ||
         (term.rdfTermDataType_ != i && term.rdfTermDataType_ != d &&
          !(term.rdfTermDataType_ == b && term.rdfTermString_.length() > 1));
}

// Appends the formatted term to `out`, with the same semantics as
// `formatTerm`, but without materializing an intermediate string.
void appendFormattedTerm(std::string& out, const EvaluatedTermData& term,
                         bool includeDataType) {
  if (!needsDatatypeWrapper(term, includeDataType)) {
    // IRI, blank node, vocab-indexed literal, or encoded value in its short
    // form: the string is already in its final form.
    out.append(term.rdfTermString_);
    return;
  }
  out.push_back('"');
  out.append(term.rdfTermString_);
  out.append("\"^^<");
  out.append(term.rdfTermDataType_);
  out.push_back('>');
}

}  // namespace

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format) {
  using enum ad_utility::MediaType;
  static constexpr std::array supportedFormats{turtle, csv, tsv, ntriples};
  AD_CONTRACT_CHECK(ad_utility::contains(supportedFormats, format));

  const auto& [subject, predicate, object] = evaluatedTriple;

  const bool includeDataType = (format == ntriples);

  // Caller-owned locals, not `thread_local`. TLS capacity is sticky for the
  // thread lifetime. A later inline into a suspendable export coroutine
  // would be S6367. The return still copies `buffer`.
  std::string buffer;
  std::string scratch;

  const EvaluatedTermData& s = *subject;
  const EvaluatedTermData& p = *predicate;
  const EvaluatedTermData& o = *object;

  // Conservative estimate of the assembled row. An escape sequence replaces
  // one character by at most two, so one extra copy of escapable terms is
  // enough.
  auto formattedTermSize = [&includeDataType](const EvaluatedTermData& term) {
    size_t size = term.rdfTermString_.size();
    if (needsDatatypeWrapper(term, includeDataType)) {
      size += 6 + std::char_traits<char>::length(term.rdfTermDataType_);
    }
    return size;
  };
  // The formatted object is a literal (and thus subject to escaping) iff it
  // either carries a datatype wrapper (which always starts with a quote) or
  // its raw string already starts with a quote.
  const bool objectIsLiteral = needsDatatypeWrapper(o, includeDataType) ||
                               ql::starts_with(o.rdfTermString_, '"');
  size_t estimatedSize =
      formattedTermSize(s) + formattedTermSize(p) + formattedTermSize(o);
  if (format == turtle || format == ntriples) {
    // Only the object can be escaped, and only if it is a literal.
    if (objectIsLiteral) {
      estimatedSize += formattedTermSize(o);
    }
    estimatedSize += 5;  // two separating spaces + " .\n"
  } else {
    // All three terms can be escaped.
    estimatedSize +=
        formattedTermSize(s) + formattedTermSize(p) + formattedTermSize(o);
    estimatedSize += 3;  // two separators + trailing newline
  }
  buffer.reserve(estimatedSize);

  if (format == turtle || format == ntriples) {
    // Only escape literals (strings starting with "). IRIs and blank nodes
    // are used as-is, avoiding an unnecessary string copy.
    appendFormattedTerm(buffer, s, includeDataType);
    buffer.push_back(' ');
    appendFormattedTerm(buffer, p, includeDataType);
    buffer.push_back(' ');
    // The object is escaped like a normalized RDF literal if it is a literal.
    // Format it into the scratch buffer first, because the escaping decision
    // depends on the formatted form.
    if (objectIsLiteral) {
      appendFormattedTerm(scratch, o, includeDataType);
      RdfEscaping::appendValidRDFLiteral(buffer, scratch);
    } else {
      appendFormattedTerm(buffer, o, includeDataType);
    }
    buffer.append(" .\n");
  } else {
    AD_CONTRACT_CHECK(format == csv || format == tsv);
    auto appendEscapedTerm = [&](const EvaluatedTermData& term) {
      scratch.clear();
      appendFormattedTerm(scratch, term, includeDataType);
      if (format == csv) {
        RdfEscaping::appendEscapedForCsv(buffer, scratch);
      } else {
        RdfEscaping::appendEscapedForTsv(buffer, scratch);
      }
    };
    appendEscapedTerm(s);
    buffer.push_back(format == csv ? ',' : '\t');
    appendEscapedTerm(p);
    buffer.push_back(format == csv ? ',' : '\t');
    appendEscapedTerm(o);
    buffer.push_back('\n');
  }
  return buffer;
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
