# Re-compiling all the infrastructure all the time is a waste of time, so we
# pre-compile that part as a library.

OPTION(_NO_BENCHMARK "Disable benchmark targets, for example when building with Emscripten." OFF)

if (NOT _NO_BENCHMARK)
    # This part compiles 'just' the benchmark infrastructure and doesn't provide a main function.
    add_library(benchmark infrastructure/Benchmark.cpp infrastructure/BenchmarkMeasurementContainer.cpp
        infrastructure/BenchmarkToJson.cpp infrastructure/BenchmarkToString.cpp)
    qlever_target_link_libraries(benchmark Boost::program_options absl::str_format sparqlParser qlever_util configManager)

    # This provides the library of the benchmark infrastructure together with its default main function.
    add_library(benchmarkWithMain infrastructure/BenchmarkMain.cpp)
    qlever_target_link_libraries(benchmarkWithMain benchmark)

    # Link binary ${basename} against the benchmark library,
    # and all libraries that are specified as additional arguments.
    function (linkBenchmark basename)
        qlever_target_link_libraries(${basename} benchmarkWithMain ${ARGN})
    endfunction()

    # Compile the benchmark and link it.
    # Usage: addAndLinkBenchmark(baseName, [additionalLibraries...])
    #   - baseName: The name of the benchmark file without the .cpp ending.
    function(addAndLinkBenchmark baseName)
        add_executable(${baseName}  "${baseName}.cpp")
        linkBenchmark(${baseName} ${ARGN})
    endfunction()

    # Add benchmarks after here.
    addAndLinkBenchmark(BenchmarkExamples)

    addAndLinkBenchmark(JoinAlgorithmBenchmark testUtil memorySize)

    addAndLinkBenchmark(ParallelMergeBenchmark testUtil)

    addAndLinkBenchmark(GroupByHashMapBenchmark engine testUtil gtest gmock)

    addAndLinkBenchmark(FsstScratchBufferBenchmark fsst)

    addAndLinkBenchmark(SerializerMicroBenchmark engine sparqlParser qlever_util)

    addAndLinkBenchmark(ChunkStreamingBenchmark engine testUtil)

    addAndLinkBenchmark(ScatterGatherBenchmark engine testUtil)

    addAndLinkBenchmark(IoUringDirectBenchmark qlever_util)

    addAndLinkBenchmark(StreamingBufferBenchmark qlever_util)

    addAndLinkBenchmark(MonomorphicSerializerBenchmark engine sparqlParser
        qlever_util)

    # The allocation-counting `operator new`/`delete` overloads in
    # `SerializerMicroBenchmark.cpp` and `MonomorphicSerializerBenchmark.cpp`
    # deliberately pair a malloc-based `operator new` with a `std::free`-based
    # `operator delete`. GCC's `-Wmismatched-new-delete` also fires at inline
    # call sites of these overloads, where a local `#pragma` cannot suppress
    # it, so disable the warning for these targets when building with GCC.
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(SerializerMicroBenchmark PRIVATE
            -Wno-mismatched-new-delete)
        target_compile_options(MonomorphicSerializerBenchmark PRIVATE
            -Wno-mismatched-new-delete)
    endif()

endif()
