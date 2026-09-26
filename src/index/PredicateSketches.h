// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#ifndef QLEVER_SRC_INDEX_PREDICATESKETCHES_H
#define QLEVER_SRC_INDEX_PREDICATESKETCHES_H

#include <optional>
#include <string>

#include "global/Id.h"
#include "index/HyperLogLogSketch.h"
#include "util/HashMap.h"
#include "util/Serializer/SerializeHashMap.h"

namespace ql::index::stats {

// The HyperLogLog sketches of the distinct subjects and the distinct objects
// of all triples with one predicate.
struct SubjectAndObjectSketches {
  HyperLogLogSketch<> subjects_;
  HyperLogLogSketch<> objects_;

  bool operator==(const SubjectAndObjectSketches& other) const {
    return subjects_ == other.subjects_ && objects_ == other.objects_;
  }

  AD_SERIALIZE_FRIEND_FUNCTION(SubjectAndObjectSketches) {
    serializer | arg.subjects_;
    serializer | arg.objects_;
  }
};

// One `SubjectAndObjectSketches` per predicate of the index. The index builder
// fills them from the triples of the PSO permutation (where all triples with
// the same predicate are adjacent) and writes them to a separate file next to
// the permutations. The query planner reads the sketches to estimate how many
// join keys two scans share (see `IndexScan::getPredicateSketch`).
class PredicateSketches {
 private:
  ad_utility::HashMap<Id, SubjectAndObjectSketches> sketches_;
  // The predicate of the previous call to `addTriple` and a pointer to its
  // sketches. Consecutive triples mostly share the predicate, so this saves
  // the hash map lookup.
  std::optional<Id> currentPredicate_;
  SubjectAndObjectSketches* currentSketches_ = nullptr;

 public:
  // Add the triple `(subject, predicate, object)` to the sketches of
  // `predicate`.
  void addTriple(Id subject, Id predicate, Id object);

  // Return the sketches of `predicate` or `nullptr` if no triple with
  // `predicate` was added.
  const SubjectAndObjectSketches* get(Id predicate) const;

  // The number of predicates with sketches.
  size_t numPredicates() const { return sketches_.size(); }

  // Write the sketches to `filename`, and read sketches that were written by
  // `writeToFile`.
  void writeToFile(const std::string& filename) const;
  static PredicateSketches readFromFile(const std::string& filename);

  bool operator==(const PredicateSketches& other) const {
    return sketches_ == other.sketches_;
  }
};

}  // namespace ql::index::stats

#endif  // QLEVER_SRC_INDEX_PREDICATESKETCHES_H
