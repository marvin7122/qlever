# All the vocabularies, including the `RdfsVocabulary` (`Vocabulary.cpp`) that
# combines them, and the `EncodedIriManager` (`EncodedIriManager.cpp`) that
# compresses/decompresses IRIs and literals.
#
# TODO<joka921> This is currently compiled as a separate library because of
# circular dependencies. It should be part of the `index` library. `VocabularyInMemory` is only used by
# the `VocabularyInMemory` and should be moved there.

add_library(vocabulary VocabularyInMemory.h VocabularyInMemory.cpp
                       VocabularyInMemoryBinSearch.cpp VocabularyInternalExternal.cpp
                       VocabularyOnDisk.cpp SplitVocabulary.cpp GeoVocabulary.cpp PolymorphicVocabulary.cpp
                       Vocabulary.cpp EncodedIriManager.cpp PrefixHeuristic.cpp)
qlever_target_link_libraries(vocabulary qlever_util rdfTypes)
