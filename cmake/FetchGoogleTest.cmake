include(FetchContent)
# CMP0135 (extract-timestamp behavior) exists only in CMake >= 3.24; setting it
# NEW there silences the author warning while staying a no-op on the 3.22 minimum.
if(POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()
FetchContent_Declare(googletest URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)
