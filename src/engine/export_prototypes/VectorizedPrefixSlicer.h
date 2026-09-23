// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#pragma once

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
#include <emmintrin.h>
#define QLEVER_SLICER_X86 1
#endif

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "backports/span.h"
#include "util/Exception.h"

namespace qlever::export_pipeline {

// _____________________________________________________________________________
// Standard known IRI prefix IDs for high-throughput single-instruction
// emission.
enum class WellKnownPrefixId : uint8_t {
  WikidataEntity = 0,  // http://www.wikidata.org/entity/
  WikidataDirectProp,  // http://www.wikidata.org/prop/direct/
  RdfSyntax,           // http://www.w3.org/1999/02/22-rdf-syntax-ns#
  RdfsSchema,          // http://www.w3.org/2000/01/rdf-schema#
  OwlOntology,         // http://www.w3.org/2002/07/owl#
  SchemaOrg,           // http://schema.org/
  XmlSchema,           // http://www.w3.org/2001/XMLSchema#
  Count
};

// _____________________________________________________________________________
// Vectorized Prefix Table: Aligns common IRI prefixes to 16-byte boundaries so
// they can be copied into chunk buffers using 1-3 SSE2/AVX instructions.
class VectorizedPrefixTable {
 public:
  struct PrefixEntry {
    alignas(16) char data[48];
    size_t length;
  };

 private:
  std::array<PrefixEntry, static_cast<size_t>(WellKnownPrefixId::Count)>
      entries_{};

 public:
  VectorizedPrefixTable() {
    initEntry(WellKnownPrefixId::WikidataEntity,
              "http://www.wikidata.org/entity/");
    initEntry(WellKnownPrefixId::WikidataDirectProp,
              "http://www.wikidata.org/prop/direct/");
    initEntry(WellKnownPrefixId::RdfSyntax,
              "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    initEntry(WellKnownPrefixId::RdfsSchema,
              "http://www.w3.org/2000/01/rdf-schema#");
    initEntry(WellKnownPrefixId::OwlOntology, "http://www.w3.org/2002/07/owl#");
    initEntry(WellKnownPrefixId::SchemaOrg, "http://schema.org/");
    initEntry(WellKnownPrefixId::XmlSchema,
              "http://www.w3.org/2001/XMLSchema#");
  }

  // ___________________________________________________________________________
  // Write a well-known prefix into `out` using 128-bit vector stores for the
  // full 16-byte vectors plus a `memcpy` tail, so exactly `entry.length`
  // bytes are written (a whole-vector store would overwrite up to 15 bytes
  // past the prefix). Returns the number of bytes written.
  [[nodiscard]] inline size_t writePrefixFast(WellKnownPrefixId id,
                                              char* out) const noexcept {
    const auto& entry = entries_[static_cast<size_t>(id)];
#ifdef QLEVER_SLICER_X86
    const __m128i* src = reinterpret_cast<const __m128i*>(entry.data);
    __m128i* dst = reinterpret_cast<__m128i*>(out);

    const size_t fullVectors = entry.length / 16;
    for (size_t i = 0; i < fullVectors; ++i) {
      _mm_storeu_si128(dst + i, _mm_load_si128(src + i));
    }
    const size_t tailBytes = entry.length % 16;
    if (tailBytes > 0) {
      std::memcpy(out + fullVectors * 16, entry.data + fullVectors * 16,
                  tailBytes);
    }
#else
    std::memcpy(out, entry.data, entry.length);
#endif
    return entry.length;
  }

  // ___________________________________________________________________________
  // Returns the static singleton instance.
  static const VectorizedPrefixTable& instance() {
    static const VectorizedPrefixTable table;
    return table;
  }

 private:
  void initEntry(WellKnownPrefixId id, std::string_view prefix) {
    auto& e = entries_[static_cast<size_t>(id)];
    AD_CONTRACT_CHECK(prefix.size() <= sizeof(e.data));
    std::memset(e.data, 0, sizeof(e.data));
    std::memcpy(e.data, prefix.data(), prefix.size());
    e.length = prefix.size();
  }
};

}  // namespace qlever::export_pipeline
