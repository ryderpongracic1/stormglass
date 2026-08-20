cmake_minimum_required(VERSION 3.22)
project(stormglass VERSION 0.1.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
option(STORMGLASS_SANITIZERS "Enable ASan + UBSan" OFF)
option(STORMGLASS_BENCH "Build benchmarks" OFF)
if(STORMGLASS_SANITIZERS)
    # Verify the sanitizer runtime actually works on this system before
    # enabling it. Some environments (e.g. minimal containers) have the
    # compiler flags but lack libasan at runtime, which fails at test
    # discovery with a confusing exit-127. Check by compiling AND running
    # a probe binary.
    include(CheckCXXSourceRuns)
    set(CMAKE_REQUIRED_FLAGS "-fsanitize=address,undefined")
    set(CMAKE_REQUIRED_LINK_OPTIONS "-fsanitize=address,undefined")
    check_cxx_source_runs("int main() { return 0; }" STORMGLASS_SANITIZER_RUNTIME_WORKS)
    unset(CMAKE_REQUIRED_FLAGS)
    unset(CMAKE_REQUIRED_LINK_OPTIONS)

    if(STORMGLASS_SANITIZER_RUNTIME_WORKS)
        add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
        add_link_options(-fsanitize=address,undefined)
        message(STATUS "stormglass: ASan + UBSan enabled")
    else()
        message(WARNING
            "stormglass: STORMGLASS_SANITIZERS=ON but the sanitizer runtime "
            "does not work on this system (libasan missing?). Building "
            "WITHOUT sanitizers. CI still runs the sanitized build.")
    endif()
endif()
include(cmake/FetchGoogleTest.cmake)
enable_testing()
add_subdirectory(src)
add_subdirectory(app)
add_subdirectory(test)
