set(OPENMATCH_SOURCES
    om_slab.c
    orderbook.c
    om_wal.c
    om_perf.c
    om_engine.c
)

add_library(openmatch_shared SHARED ${OPENMATCH_SOURCES})
add_library(openmatch_static STATIC ${OPENMATCH_SOURCES})

add_library(openmatch ALIAS openmatch_shared)

if(USE_KHASHL)
    target_compile_definitions(openmatch_shared PRIVATE OM_USE_KHASHL)
    target_compile_definitions(openmatch_static PRIVATE OM_USE_KHASHL)
    target_sources(openmatch_shared PRIVATE om_hash_khashl.c)
    target_sources(openmatch_static PRIVATE om_hash_khashl.c)
else()
    target_sources(openmatch_shared PRIVATE om_hash_khash.c)
    target_sources(openmatch_static PRIVATE om_hash_khash.c)
endif()

target_include_directories(openmatch_shared
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(openmatch_static
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(openmatch_shared PROPERTIES
    OUTPUT_NAME openmatch
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmatch/om_slab.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_hash.h;${CMAKE_SOURCE_DIR}/include/openmatch/orderbook.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_wal.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_perf.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_engine.h"
)

set_target_properties(openmatch_static PROPERTIES
    OUTPUT_NAME openmatch
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmatch/om_slab.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_hash.h;${CMAKE_SOURCE_DIR}/include/openmatch/orderbook.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_wal.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_perf.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_engine.h"
)

install(TARGETS openmatch_shared openmatch_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include/openmatch
)
