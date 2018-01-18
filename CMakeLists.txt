cmake_minimum_required(VERSION 2.8.10)
project(braft C CXX)

#option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

include(FindThreads)
include(FindProtobuf)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()

find_path(GFLAGS_INCLUDE_PATH NAMES gflags/gflags.h)
find_library(GFLAGS_LIB NAMES gflags)
if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIB))
    message(FATAL_ERROR "Fail to find gflags")
endif()

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a)
if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()

#execute_process(
#    COMMAND bash -c "find ${CMAKE_SOURCE_DIR}/../.. -type d -name \"include\" | xargs dirname | tr -d '\n'"
#    OUTPUT_VARIABLE OUTPUT_PATH
#)

#set(CMAKE_PREFIX_PATH ${OUTPUT_PATH})

find_library(PROTOC_LIB NAMES protoc)
if(NOT PROTOC_LIB)
    message(FATAL_ERROR "Fail to find protoc lib")
endif()

if (NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTOC_LIB_DIR ${PROTOC_LIB} DIRECTORY)
    set (PROTOBUF_PROTOC_EXECUTABLE "${PROTOC_LIB_DIR}/../bin/protoc")
endif()

include_directories(
        ${GFLAGS_INCLUDE_PATH}
        ${PROTOBUF_INCLUDE_DIRS}
        ${LEVELDB_INCLUDE_PATH}
        ${BRPC_INCLUDE_PATH}
        )

message ("protobuf_lib is ${PROTOBUF_LIBRARY} sdlkfjlskjgds")

set(DYNAMIC_LIB
    ${GFLAGS_LIB}
    ${PROTOBUF_LIBRARY}
    ${LEVELDB_LIB}
    ${PROTOC_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${BRPC_LIB}
    rt
    ssl
    crypto
    dl
    z
    )

# for *.so
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/lib)
# for *.a
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/lib)

file(GLOB BRAFT_PROTOS "${CMAKE_SOURCE_DIR}/src/braft/*.proto")
foreach(PROTO ${BRAFT_PROTOS})
    message("file is ${PROTO}")
    message("protc is ${PROTOBUF_PROTOC_EXECUTABLE}")
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/braft/${PROTO_WE}.pb.cc")
    execute_process(
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS}
        --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
        --proto_path=${PROTOBUF_INCLUDE_DIR}
        --proto_path=${CMAKE_SOURCE_DIR}/src
        --proto_path=${CMAKE_SOURCE_DIR}/src/braft/ ${PROTO}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )
endforeach()
file(GLOB_RECURSE BRAFT_SOURCES "${CMAKE_SOURCE_DIR}/src/braft/*.cpp")
set(SOURCES
    ${BRAFT_SOURCES}
    ${PROTO_SRCS}
    )

include_directories(
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
)

execute_process(
    COMMAND bash -c "git rev-parse --short HEAD | tr -d '\n'"
    OUTPUT_VARIABLE BRAFT_REVISION
)

set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DBTHREAD_USE_FAST_PTHREAD_MUTEX -D__const__= -D_GNU_SOURCE -DUSE_SYMBOLIZE -DNO_TCMALLOC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DBRAFT_REVISION=\\\"${BRAFT_REVISION}\\\" -D__STRICT_ANSI__")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${DEBUG_SYMBOL}")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -msse4 -msse4.2")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS "${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer")

macro(use_cxx11)
if(CMAKE_VERSION VERSION_LESS "3.1.3")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()
endmacro(use_cxx11)

use_cxx11()

add_subdirectory(src)
if(BUILD_UNIT_TESTS)
    add_subdirectory(test)
endif()
add_subdirectory(tools)
