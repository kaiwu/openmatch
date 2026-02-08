set(OPENMATCH_SOURCES
    om_slab.c
    orderbook.c
    om_perf.c
    om_engine.c
)

option(OM_USE_WAL_MOCK "Use WAL mock implementation" OFF)

set(OPENMATCH_WAL_SOURCES om_wal.c)
if(OM_USE_WAL_MOCK)
    set(OPENMATCH_WAL_SOURCES om_wal_mock.c)
    add_compile_definitions(OM_USE_WAL_MOCK)
endif()

add_library(openmatch_shared SHARED ${OPENMATCH_SOURCES} ${OPENMATCH_WAL_SOURCES})
add_library(openmatch_static STATIC ${OPENMATCH_SOURCES} ${OPENMATCH_WAL_SOURCES})

add_library(openmatch ALIAS openmatch_shared)

set(OPENMARKET_SOURCES
    om_market.c
    om_worker.c
)

add_library(openmarket_shared SHARED ${OPENMARKET_SOURCES})
add_library(openmarket_static STATIC ${OPENMARKET_SOURCES})

add_library(openmarket ALIAS openmarket_shared)

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

target_include_directories(openmarket_shared
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(openmarket_static
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

set_target_properties(openmarket_shared PROPERTIES
    OUTPUT_NAME openmarket
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmarket/om_market.h;${CMAKE_SOURCE_DIR}/include/openmarket/om_worker.h"
)

set_target_properties(openmarket_static PROPERTIES
    OUTPUT_NAME openmarket
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmarket/om_market.h;${CMAKE_SOURCE_DIR}/include/openmarket/om_worker.h"
)

install(TARGETS openmatch_shared openmatch_static openmarket_shared openmarket_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include
)

# ---------- ombus (message bus) ----------
set(OMBUS_SOURCES om_bus_shm.c)

add_library(ombus_shared SHARED ${OMBUS_SOURCES})
add_library(ombus_static STATIC ${OMBUS_SOURCES})
add_library(ombus ALIAS ombus_shared)

target_link_libraries(ombus_shared rt)
target_link_libraries(ombus_static rt)

target_include_directories(ombus_shared
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(ombus_static
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(ombus_shared PROPERTIES
    OUTPUT_NAME ombus
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/ombus/om_bus.h;${CMAKE_SOURCE_DIR}/include/ombus/om_bus_error.h"
)

set_target_properties(ombus_static PROPERTIES
    OUTPUT_NAME ombus
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/ombus/om_bus.h;${CMAKE_SOURCE_DIR}/include/ombus/om_bus_error.h"
)

install(TARGETS ombus_shared ombus_static
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include/ombus
)
