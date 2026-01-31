find_package(Threads REQUIRED)

set(TEST_SOURCES
    test_slab.c
    test_engine.c
)

foreach(test_source ${TEST_SOURCES})
    get_filename_component(test_name ${test_source} NAME_WE)
    add_executable(${test_name} ${test_source})
    target_link_libraries(${test_name} openmatch Threads::Threads)
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
