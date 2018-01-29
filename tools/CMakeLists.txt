set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG -O2 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/output/bin)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
add_executable(braft_cli braft_cli.cpp)
target_link_libraries(braft_cli 
                      "-Xlinker \"-(\""
                      braft-static
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\""
                      )
