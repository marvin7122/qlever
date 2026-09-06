
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#include <emmintrin.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "backports/span.h"
#include "util/Exception.h"

namespace qlever::export_prototypes {

// _____________________________________________________________________________
// Single source of truth for well-known IRI prefixes.
constexpr std::string_view kWikidataEntityPrefix = "http://www.wikidata.org/entity/";
constexpr std::string_view kWikidataDirectPropPrefix = "http://www.wikidata.org/prop/direct/";
constexpr std::string_view kRdfSyntaxPrefix = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
constexpr std::string_view kRdfsSchemaPrefix = "http://www.w3.org/2000/01/rdf-schema#";
constexpr std::string_view kOwlOntologyPrefix = "http://www.w3.org/2002/07/owl#";
constexpr std::string_view kSchemaOrgPrefix = "http://schema.org/";
constexpr std::string_view kXmlSchemaPrefix = "http://www.w3.org/2001/XMLSchema#";

// _____________________________________________________________________________
// Define standard known IRI prefix IDs for high-throughput vectorized emission.

  XmlSchema,             // http://www.w3.org/2001/XMLSchema#
  Count
};

// _____________________________________________________________________________
// Align common IRI prefixes to 16-byte boundaries for vectorized copy into chunk buffers.
class VectorizedPrefixTable {
 public:
    // Stores a prefix aligned to 16 bytes with its length.
  alignas(16) struct PrefixEntry {
    alignas(16) char data[48];
    uint8_t length;
  };

 private:
  std::array<PrefixEntry, static_cast<size_t>(WellKnownPrefixId::Count)> entries_{};

 public:
  VectorizedPrefixTable() noexcept {
    initEntry(WellKnownPrefixId::WikidataEntity, kWikidataEntityPrefix);
    initEntry(WellKnownPrefixId::WikidataDirectProp, kWikidataDirectPropPrefix);
    initEntry(WellKnownPrefixId::RdfSyntax, kRdfSyntaxPrefix);
    initEntry(WellKnownPrefixId::RdfsSchema, kRdfsSchemaPrefix);
    initEntry(WellKnownPrefixId::OwlOntology, kOwlOntologyPrefix);
    initEntry(WellKnownPrefixId::SchemaOrg, kSchemaOrgPrefix);
    initEntry(WellKnownPrefixId::XmlSchema, kXmlSchemaPrefix);
  }

  // ___________________________________________________________________________
  // Write a well-known prefix into `out` using 128-bit vector stores.
  // Return the number of bytes written.
  [[nodiscard]] inline size_t writePrefixFast(WellKnownPrefixId id, char* out) const noexcept {
    AD_CHECK(out != nullptr);
    AD_CHECK(static_cast<size_t>(id) < static_cast<size_t>(WellKnownPrefixId::Count));
    const size_t idx = static_cast<size_t>(id);  // out must point to a buffer of at least 48 bytes

    const auto& entry = entries_[idx];
    AD_CONTRACT_CHECK(out != nullptr);
    AD_CONTRACT_CHECK(entry.length <= sizeof(entry.data));
    std::memcpy(out, entry.data, entry.length);
    return entry.length;
  }

    // ___________________________________________________________________________
  static const VectorizedPrefixTable& instance() noexcept {
    static const VectorizedPrefixTable table;
    return table;
  }

 private:
  // Initialize a PrefixEntry by zero-filling the data buffer and copying the prefix.
  void initEntry(WellKnownPrefixId id, std::string_view prefix) noexcept {
    auto& e = entries_[static_cast<size_t>(id)];
    AD_CONTRACT_CHECK(prefix.size() <= sizeof(e.data));
    std::memset(e.data, 0, sizeof(e.data));
    std::memcpy(e.data, prefix.data(), prefix.size());
    e.length = static_cast<uint8_t>(prefix.size());
    AD_CONTRACT_CHECK(e.length <= sizeof(e.data));
  }
};

}  // namespace qlever::export_pipeline
