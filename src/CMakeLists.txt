set(OPENMATCH_SOURCES
    om_slab.c
)

add_library(openmatch ${OPENMATCH_SOURCES})

if(USE_KHASHL)
    target_compile_definitions(openmatch PRIVATE OM_USE_KHASHL)
    target_sources(openmatch PRIVATE om_hash_khashl.c)
else()
    target_sources(openmatch PRIVATE om_hash_khash.c)
endif()

target_include_directories(openmatch
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(openmatch PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmatch/om_slab.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_hash.h"
)

install(TARGETS openmatch
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include/openmatch
)
