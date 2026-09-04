// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleInstantiator.h"

#include <array>
#include <charconv>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/ConstructDeduplicator.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Algorithm.h"
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
          return EvaluatedTerm::blankNode(*t.prefix_, *t.suffix_, rowIdxTotal);
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

void appendBlankNode(std::string& out, std::string_view prefix, size_t row,
                     std::string_view suffix) {
  out.append(prefix);
  char digits[24];
  auto [end, err] = std::to_chars(digits, digits + sizeof(digits), row);
  AD_CORRECTNESS_CHECK(err == std::errc{});
  out.append(digits, static_cast<size_t>(end - digits));
  out.append(suffix);
}

void appendFormattedTermData(std::string& out, const EvaluatedTermData& term,
                             bool includeDataType) {
  if (!needsDatatypeWrapper(term, includeDataType)) {
    out.append(term.rdfTermString_);
    return;
  }
  out.push_back('"');
  out.append(term.rdfTermString_);
  out.append("\"^^<");
  out.append(term.rdfTermDataType_);
  out.push_back('>');
}

size_t formattedTermSize(const EvaluatedTerm& term, bool includeDataType) {
  if (term.isBlankNode()) {
    // 20 digits is enough for any 64-bit decimal row id.
    return term.blankPrefix().size() + 20 + term.blankSuffix().size();
  }
  size_t size = term->rdfTermString_.size();
  if (needsDatatypeWrapper(*term, includeDataType)) {
    size += 6 + std::char_traits<char>::length(term->rdfTermDataType_);
  }
  return size;
}

bool termIsLiteral(const EvaluatedTerm& term, bool includeDataType) {
  if (term.isBlankNode()) {
    return false;
  }
  return needsDatatypeWrapper(*term, includeDataType) ||
         ql::starts_with(term->rdfTermString_, '"');
}

) and wrap it as `rdfTermString_`) and
// wrap it as `"escaped"^^<datatype>`.
void appendTypedLiteral(std::string& out, const EvaluatedTermData& term) {
  out.push_back('"');
  // `appendValidRDFLiteral` expects a normalized `"content"` literal. Build
  // that view without a heap string when the raw value has no quotes.
  if (term.rdfTermString_.find_first_of("\\\"\n\r") == std::string::npos) {
    out.append(term.rdfTermString_);
  } else {
    std::string wrapped;
    wrapped.reserve(term.rdfTermString_.size() + 2);
    wrapped.push_back('"');
    wrapped.append(term.rdfTermString_);
    wrapped.push_back('"');
    std::string escaped;
    RdfEscaping::appendValidRDFLiteral(escaped, wrapped);
    // `escaped` is `"content"`; drop the surrounding quotes, already written.
    AD_CORRECTNESS_CHECK(escaped.size() >= 2 && escaped.front() == '"' &&
                         escaped.back() == '"');
    out.append(escaped.data() + 1, escaped.size() - 2);
  }
  out.append("\"^^<");
  out.append(term.rdfTermDataType_);
  out.push_back('>');
}

void appendTurtleObject(std::string& out, const EvaluatedTerm& object,
                        bool includeDataType) {
  if (!termIsLiteral(object, includeDataType)) {
    appendFormattedTerm(out, object, includeDataType);
    return;
  }
  if (needsDatatypeWrapper(*object, includeDataType)) {
    appendTypedLiteral(out, *object);
    return;
  }
  RdfEscaping::appendValidRDFLiteral(out, object->rdfTermString_);
}

void appendCsvOrTsvTerm(std::string& out, const EvaluatedTerm& term,
                        bool includeDataType, bool csv) {
  const size_t begin = out.size();
  appendFormattedTerm(out, term, includeDataType);
  std::string_view formatted{out.data() + begin, out.size() - begin};
  if (csv) {
    if (!formatted.empty() &&
        formatted.find_first_of("\r\n\",") != std::string_view::npos) {
      std::string copy{formatted};
      out.resize(begin);
      RdfEscaping::appendEscapedForCsv(out, copy);
    }
  } else if (!formatted.empty() &&
             formatted.find_first_of("\n\t") != std::string_view::npos) {
    std::string copy{formatted};
    out.resize(begin);
    RdfEscaping::appendEscapedForTsv(out, copy);
  }
}
}  // namespace

// _____________________________________________________________________________
void appendFormattedTerm(std::string& out, const EvaluatedTerm& term,
                         bool includeDataType) {
  if (term.isBlankNode()) {
    appendBlankNode(out, term.blankPrefix(), term.blankRow(),
                    term.blankSuffix());
    return;
  }
  appendFormattedTermData(out, *term, includeDataType);
}

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTermData& term, bool includeDataType) {
  std::string out;
  appendFormattedTermData(out, term, includeDataType);
  return out;
}

// _____________________________________________________________________________
std::string formatTerm(const EvaluatedTerm& term, bool includeDataType) {
  std::string out;
  appendFormattedTerm(out, term, includeDataType);
  return out;
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

  const size_t start = out.size();
  size_t estimatedSize = formattedTermSize(subject, includeDataType) +
                         formattedTermSize(predicate, includeDataType) +
                         formattedTermSize(object, includeDataType);
  if (format == turtle || format == ntriples) {
    if (termIsLiteral(object, includeDataType)) {
      estimatedSize += formattedTermSize(object, includeDataType);
    }
    estimatedSize += 5;
  } else {
    estimatedSize += formattedTermSize(subject, includeDataType) +
                     formattedTermSize(predicate, includeDataType) +
                     formattedTermSize(object, includeDataType) + 3;
  }
  out.reserve(start + estimatedSize);

  if (format == turtle || format == ntriples) {
    appendFormattedTerm(out, subject, includeDataType);
    out.push_back(' ');
    appendFormattedTerm(out, predicate, includeDataType);
    out.push_back(' ');
    appendTurtleObject(out, object, includeDataType);
    out.append(" .\n");
    return;
  }

  AD_CONTRACT_CHECK(format == csv || format == tsv);
  const bool isCsv = format == ad_utility::MediaType::csv;
  appendCsvOrTsvTerm(out, subject, includeDataType, isCsv);
  out.push_back(isCsv ? ',' : '\t');
  appendCsvOrTsvTerm(out, predicate, includeDataType, isCsv);
  out.push_back(isCsv ? ',' : '\t');
  appendCsvOrTsvTerm(out, object, includeDataType, isCsv);
  out.push_back('\n');
}

// _____________________________________________________________________________
std::string formatTriple(const EvaluatedTriple& evaluatedTriple,
                         const ad_utility::MediaType& format) {
  std::string buffer;
  appendFormattedTriple(buffer, evaluatedTriple, format);
  return buffer;
}

// _____________________________________________________________________________
StringTriple createStringTriple(const EvaluatedTriple& evaluatedTriple,
                                bool includeDataType) {
  const auto& [subject, predicate, object] = evaluatedTriple;
  return StringTriple{formatTerm(subject, includeDataType),
                      formatTerm(predicate, includeDataType),
                      formatTerm(object, includeDataType)};
}
}  // namespace qlever::constructExport
