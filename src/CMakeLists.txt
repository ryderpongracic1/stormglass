file(GLOB_RECURSE STORMGLASS_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
add_library(stormglass_lib STATIC ${STORMGLASS_SOURCES})
target_include_directories(stormglass_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# PartitionedPipeline spawns std::thread workers. Link the platform thread
# library PUBLIC so every consumer of stormglass_lib (tests, apps) also links it.
find_package(Threads REQUIRED)
target_link_libraries(stormglass_lib PUBLIC Threads::Threads)
