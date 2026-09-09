# All vocabulary implementations, including `RdfsVocabulary` (`Vocabulary.cpp`)
# and `EncodedIriManager` (`EncodedIriManager.cpp`), are compiled into this
# library. Keep this as a separate target from the index library.

add_library(vocabulary VocabularyInMemory.h VocabularyInMemory.cpp
                       VocabularyInMemoryBinSearch.cpp VocabularyInternalExternal.cpp
                       VocabularyOnDisk.cpp SplitVocabulary.cpp GeoVocabulary.cpp PolymorphicVocabulary.cpp
                       Vocabulary.cpp EncodedIriManager.cpp PrefixHeuristic.cpp
                       SecondaryVocabulary.cpp)
qlever_target_link_libraries(vocabulary qlever_util rdfTypes)
