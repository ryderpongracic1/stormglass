add_executable(stormglass_demo stormglass/main.cpp)
target_link_libraries(stormglass_demo PRIVATE stormglass_lib)

add_executable(stormglass_bench bench/main.cpp)
target_link_libraries(stormglass_bench PRIVATE stormglass_lib)
target_compile_options(stormglass_bench PRIVATE -O3 -march=native)

add_executable(stormglass_scanbench bench/scan_main.cpp)
target_link_libraries(stormglass_scanbench PRIVATE stormglass_lib)
target_compile_options(stormglass_scanbench PRIVATE -O3 -march=native)

add_executable(stormglass_compare bench/compare_main.cpp)
target_link_libraries(stormglass_compare PRIVATE stormglass_lib)
target_compile_options(stormglass_compare PRIVATE -O3 -march=native)

add_executable(stormglass_oracle oracle/main.cpp)
target_link_libraries(stormglass_oracle PRIVATE stormglass_lib)

add_executable(stormglass_nemesis nemesis/main.cpp)
target_link_libraries(stormglass_nemesis PRIVATE stormglass_lib)

add_executable(stormglass_snapshot_demo snapshot/main.cpp)
target_link_libraries(stormglass_snapshot_demo PRIVATE stormglass_lib)
add_test(NAME TcpSnapshot.ProcessRecovery COMMAND stormglass_snapshot_demo)
add_test(NAME TcpSnapshot.MidMarkerCrashRecovery COMMAND stormglass_snapshot_demo --crash)
set_tests_properties(TcpSnapshot.ProcessRecovery TcpSnapshot.MidMarkerCrashRecovery PROPERTIES TIMEOUT 45)
