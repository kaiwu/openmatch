set(OPENMATCH_SOURCES
    om_slab.c
    om_engine.c
)

if(ENABLE_F14)
    list(APPEND OPENMATCH_SOURCES om_hash_f14.c)
    target_compile_definitions(openmatch PRIVATE OM_USE_F14)
else()
    list(APPEND OPENMATCH_SOURCES om_hash_khash.c)
endif()

add_library(openmatch ${OPENMATCH_SOURCES})

target_include_directories(openmatch
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

set_target_properties(openmatch PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    PUBLIC_HEADER "${CMAKE_SOURCE_DIR}/include/openmatch/om_slab.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_hash.h;${CMAKE_SOURCE_DIR}/include/openmatch/om_engine.h"
)

install(TARGETS openmatch
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    PUBLIC_HEADER DESTINATION include/openmatch
)
