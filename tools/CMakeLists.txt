add_executable(wal_reader wal_reader.c)

target_include_directories(wal_reader
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(wal_reader
    PRIVATE
        openmatch
)
