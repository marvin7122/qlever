// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#include "index/PredicateSketches.h"

#include "util/Serializer/FileSerializer.h"

namespace ql::index::stats {

// _____________________________________________________________________________
void PredicateSketches::addTriple(Id subject, Id predicate, Id object) {
  if (currentPredicate_ != predicate) {
    currentPredicate_ = predicate;
    currentSketches_ = &sketches_[predicate];
  }
  currentSketches_->subjects_.insert(subject);
  currentSketches_->objects_.insert(object);
}

// _____________________________________________________________________________
const SubjectAndObjectSketches* PredicateSketches::get(Id predicate) const {
  auto it = sketches_.find(predicate);
  return it == sketches_.end() ? nullptr : &it->second;
}

// _____________________________________________________________________________
void PredicateSketches::writeToFile(const std::string& filename) const {
  ad_utility::serialization::FileWriteSerializer serializer{filename};
  serializer << sketches_;
}

// _____________________________________________________________________________
PredicateSketches PredicateSketches::readFromFile(const std::string& filename) {
  ad_utility::serialization::FileReadSerializer serializer{filename};
  PredicateSketches result;
  serializer >> result.sketches_;
  return result;
}

}  // namespace ql::index::stats
