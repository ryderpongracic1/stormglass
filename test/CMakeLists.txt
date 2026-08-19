file(GLOB TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*_test.cpp")

add_executable(stormglass_tests ${TEST_SOURCES})
target_link_libraries(stormglass_tests PRIVATE stormglass_lib GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(stormglass_tests)
