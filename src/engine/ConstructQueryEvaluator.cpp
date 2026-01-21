#include "ConstructQueryEvaluator.h"

#include "ExportQueryExecutionTrees.h"

std::optional<std::string> ConstructQueryEvaluator::evaluate(const Iri& iri) {
  return iri.iri();
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Literal& literal, PositionInTriple role) {
  if (role == PositionInTriple::OBJECT) {
    return literal.literal();
  }
  return std::nullopt;
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const Variable& var, const ConstructQueryExportContext& context) {
  auto& cache = context.variableCache_;
  const std::string& varName = var.name();

  // Check cache first - avoids repeated expensive idToStringAndType lookups
  // when the same variable appears multiple times in the CONSTRUCT template.
  if (auto it = cache.find(varName); it != cache.end()) {
    return it->second;
  }

  std::optional<std::string> result = std::nullopt;

  const auto& variableColumns = context._variableColumns;
  if (variableColumns.contains(var)) {
    size_t index = variableColumns.at(var).columnIndex_;
    auto id = context.idTable_(context._resultTableRowIdx, index);
    auto optionalStringAndType = ExportQueryExecutionTrees::idToStringAndType(
        context._qecIndex, id, context.localVocab_);

    if (optionalStringAndType.has_value()) {
      auto& [literal, type] = optionalStringAndType.value();
      const char* i = XSD_INT_TYPE;
      const char* d = XSD_DECIMAL_TYPE;
      const char* b = XSD_BOOLEAN_TYPE;

      // Note: If `type` is `XSD_DOUBLE_TYPE`, `literal` is always "NaN", "INF"
      // or "-INF", which doesn't have a short form notation.
      if (type == nullptr || type == i || type == d ||
          (type == b && literal.length() > 1)) {
        result = std::move(literal);
      } else {
        result = absl::StrCat("\"", literal, "\"^^<", type, ">");
      }
    }
  }

  // Store in cache and return
  cache[varName] = result;
  return result;
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const BlankNode& node, const ConstructQueryExportContext& context) {
  std::ostringstream stream;
  stream << "_:";
  stream << (node.isGenerated() ? 'g' : 'u');  // generated or user-defined
  stream << context._rowOffset + context._resultTableRowIdx << '_';
  stream << node.label();
  return stream.str();
}

std::optional<std::string> ConstructQueryEvaluator::evaluate(
    const GraphTerm& term, const ConstructQueryExportContext& context,
    PositionInTriple posInTriple) {
  if (std::holds_alternative<Variable>(term)) {
    const Variable& var = std::get<Variable>(term);
    return ConstructQueryEvaluator::evaluate(var, context);
  }

  if (std::holds_alternative<BlankNode>(term)) {
    const BlankNode& node = std::get<BlankNode>(term);
    return ConstructQueryEvaluator::evaluate(node, context);
  }

  if (std::holds_alternative<Iri>(term)) {
    const Iri& iri = std::get<Iri>(term);
    return ConstructQueryEvaluator::evaluate(iri);
  }

  if (std::holds_alternative<Literal>(term)) {
    const Literal& literal = std::get<Literal>(term);
    return ConstructQueryEvaluator::evaluate(literal, posInTriple);
  }

  AD_FAIL();
}
