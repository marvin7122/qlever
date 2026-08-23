# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION ${CMAKE_VERSION}) # this file comes with cmake

# If CMAKE_DISABLE_SOURCE_CHANGES is set to true and the source directory is an
# existing directory in our source tree, calling file(MAKE_DIRECTORY) on it
# would cause a fatal error, even though it would be a no-op.
if(NOT EXISTS "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-src")
  file(MAKE_DIRECTORY "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-src")
endif()
file(MAKE_DIRECTORY
  "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-build"
  "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix"
  "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix/tmp"
  "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix/src/spatialjoin-populate-stamp"
  "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix/src"
  "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix/src/spatialjoin-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix/src/spatialjoin-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/userNoPriv/code/qlever/qlever-code/build-review/_deps/spatialjoin-subbuild/spatialjoin-populate-prefix/src/spatialjoin-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
