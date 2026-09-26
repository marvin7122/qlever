// Copyright 2019 - 2026 The QLever Authors, in particular:
//
// 2019 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Only performs the "mergeVocabulary" step of the IndexBuilder pipeline
// Can be used e.g. for benchmarking this step to develop faster IndexBuilders.

#include "global/FileSuffixConstants.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary/Vocabulary.h"

// ____________________________________________________________________________________________________
int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr
        << "Usage: " << argv[0]
        << "<basename of index> <number of partial vocabulary files to merge>";
  }
  std::string basename = argv[1];
  size_t numFiles = atoi(argv[2]);

  auto file = ad_utility::makeOfstream(basename + VOCAB_SUFFIX);
  uint64_t count = 0;
  auto wordCallback = [&file, &count](
                          const auto& word,
                          [[maybe_unused]] bool isExternalDummy = true) {
    file << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
    return count++;
  };

  ad_utility::vocabulary::TripleComponentComparator comparator;
  ad_utility::vocabulary_merger::mergeVocabulary(
      basename, numFiles,
      [&comparator](std::string_view a, bool aIsExternal, std::string_view b,
                    bool bIsExternal) {
        return comparator.isLessInTotalWithExternalFlag(a, aIsExternal, b,
                                                        bIsExternal);
      },
      wordCallback, 4_GB);
}
