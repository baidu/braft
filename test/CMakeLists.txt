find_package(Gperftools)
include_directories(${GPERFTOOLS_INCLUDE_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/test)

find_path(GTEST_HEADER NAMES gtest/gtest.h)
find_library(GTEST_LIB NAMES gtest)
find_library(GTEST_MAIN_LIB NAMES gtest_main)

set(CMAKE_CPP_FLAGS "-DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -D__const__= -D_GNU_SOURCE -DUSE_SYMBOLIZE -DNO_TCMALLOC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DUNIT_TEST -g -Dprivate=public -Dprotected=public -D__STRICT_ANSI__ -include sstream_workaround.h")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer -Wno-unused-result")
use_cxx11()

# bthread_* functions are used in logging.cc, and they need to be marked as
# weak symbols explicitly in Darwin system.
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        "-Wl,-U,_bthread_getspecific"
        "-Wl,-U,_bthread_setspecific"
        "-Wl,-U,_bthread_key_create")
endif()

file(GLOB TEST_BRAFT_SRCS "test_*.cpp")
foreach(BRAFT_UT ${TEST_BRAFT_SRCS})
    get_filename_component(BRAFT_UT_WE ${BRAFT_UT} NAME_WE)
    add_executable(${BRAFT_UT_WE} ${BRAFT_UT} 
                   $<TARGET_OBJECTS:OBJ_LIB>)
    target_link_libraries(${BRAFT_UT_WE}
        #"-Xlinker \"-(\""
                          ${GTEST_MAIN_LIB}
                          ${GTEST_LIB}
                          ${GPERFTOOLS_LIBRARY}
                          ${DYNAMIC_LIB}
                          #                  "-Xlinker \"-)\""
                          )
endforeach()
