find_package(Threads REQUIRED)

set(TEST_SOURCES
    test_runner.c
    test_slab.c
    test_orderbook.c
    test_wal.c
    test_engine.c
    test_market.c
)

add_executable(test_runner ${TEST_SOURCES})
target_link_libraries(test_runner openmatch openmarket check Threads::Threads)
target_include_directories(test_runner PRIVATE ${CMAKE_SOURCE_DIR}/deps/check/src ${CMAKE_BINARY_DIR}/deps/check)

add_test(NAME test_runner COMMAND test_runner)
