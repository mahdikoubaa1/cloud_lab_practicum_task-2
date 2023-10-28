cmake_minimum_required(VERSION 3.14)
project(cloudlab
        VERSION 0.1
        DESCRIPTION "TUM Cloud Computing Lab"
        LANGUAGES CXX)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
find_package(RocksDB REQUIRED)
find_package(Protobuf REQUIRED)
find_package(LibEvent REQUIRED)

FetchContent_Declare(fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG 9.0.0)

FetchContent_MakeAvailable(fmt)

# compile protobuf messages
protobuf_generate_cpp(PROTO_SRC PROTO_HDR ${PROJECT_SOURCE_DIR}/lib/message/cloud.proto)

# cloudlab library
add_library(cloudlab lib/network/server.cc include/cloudlab/network/server.hh lib/kvs.cc include/cloudlab/kvs.hh include/cloudlab/network/address.hh ${PROTO_SRC} ${PROTO_HDR} lib/handler/api.cc include/cloudlab/handler/api.hh lib/handler/p2p.cc include/cloudlab/handler/p2p.hh include/cloudlab/handler/handler.hh lib/network/connection.cc include/cloudlab/network/connection.hh include/cloudlab/spmc.hh lib/network/address.cc lib/handler/router.cc)
target_include_directories(cloudlab PUBLIC ${PROJECT_SOURCE_DIR}/include PRIVATE ${PROJECT_SOURCE_DIR}/lib PUBLIC ${CMAKE_CURRENT_BINARY_DIR} PUBLIC ${PROTOBUF_INCLUDE_DIR} PRIVATE ${LIBEVENT_INCLUDE_DIR})
target_link_libraries(cloudlab PUBLIC ${PROTOBUF_LIBRARY} PRIVATE fmt::fmt PRIVATE ${ROCKSDB_LIBRARY} PRIVATE Threads::Threads PRIVATE ${LIBEVENT_LIBRARY})

# ctl executable
add_executable(ctl-test src/ctl.cc src/argh.hh)
target_link_libraries(ctl-test cloudlab fmt::fmt)

# router executable
add_executable(router-test src/router.cc src/argh.hh)
target_link_libraries(router-test cloudlab fmt::fmt)

# kvs executable
add_executable(kvs-test src/kvs.cc src/argh.hh)
target_link_libraries(kvs-test cloudlab fmt::fmt)