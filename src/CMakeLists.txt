file(GLOB_RECURSE STORMGLASS_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
add_library(stormglass_lib STATIC ${STORMGLASS_SOURCES})
target_include_directories(stormglass_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(stormglass_lib PRIVATE -msse4.2)
