# Copyright (C) 2026 EloqData Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.20)

project(celer VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(CELER_BUILD_EXAMPLES "Build example applications" ON)
option(CELER_ENABLE_WERROR "Treat warnings as errors" OFF)
# Selects the network backend at compile time (see io/net_backend.h). Default OFF
# = io_uring. DPDK is not implemented yet; turning this on is reserved for it.
option(CELER_WITH_DPDK "Build with the DPDK net backend instead of io_uring" OFF)

set(CELER_LIBURING_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/liburing")
if (NOT EXISTS "${CELER_LIBURING_SOURCE_DIR}/configure")
  message(FATAL_ERROR
    "liburing submodule not found; run git submodule update --init --recursive")
endif()

include(ExternalProject)
find_program(CELER_MAKE_EXECUTABLE NAMES gmake make REQUIRED)

set(CELER_LIBURING_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/liburing")
set(CELER_LIBURING_ARCHIVE "${CELER_LIBURING_BINARY_DIR}/src/liburing.a")
# CMake validates imported include paths during generation, before liburing's
# configure script has created its generated-header directory.
file(MAKE_DIRECTORY "${CELER_LIBURING_BINARY_DIR}/src/include")

ExternalProject_Add(celer_liburing_build
  SOURCE_DIR "${CELER_LIBURING_SOURCE_DIR}"
  BINARY_DIR "${CELER_LIBURING_BINARY_DIR}"
  DOWNLOAD_COMMAND ""
  UPDATE_COMMAND ""
  PATCH_COMMAND ""
  CONFIGURE_COMMAND
    "${CELER_LIBURING_SOURCE_DIR}/configure"
      "--cc=${CMAKE_C_COMPILER}"
      "--cxx=${CMAKE_CXX_COMPILER}"
  BUILD_COMMAND
    "${CELER_MAKE_EXECUTABLE}" -C "${CELER_LIBURING_BINARY_DIR}/src"
      liburing.a ENABLE_SHARED=0
  INSTALL_COMMAND ""
  BUILD_BYPRODUCTS "${CELER_LIBURING_ARCHIVE}"
)

add_library(celer_liburing STATIC IMPORTED GLOBAL)
set_target_properties(celer_liburing PROPERTIES
  IMPORTED_LOCATION "${CELER_LIBURING_ARCHIVE}"
  INTERFACE_INCLUDE_DIRECTORIES
    "${CELER_LIBURING_BINARY_DIR}/src/include;${CELER_LIBURING_SOURCE_DIR}/src/include"
)
add_dependencies(celer_liburing celer_liburing_build)

add_library(celer
  src/io/io.cpp
  src/io/storage.cpp
  src/io/io_uring_backend.cpp
  src/net/server.cpp
  src/net/tcp_service.cpp
  src/runtime/worker.cpp
  src/runtime/runtime.cpp
  src/net/tcp_listener.cpp
  src/net/tcp_stream.cpp
)

add_library(celer::celer ALIAS celer)
add_library(celer::core ALIAS celer)
add_library(celer::io ALIAS celer)

target_include_directories(celer
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party  # vendored header-only spdlog
)

target_compile_features(celer PUBLIC cxx_std_23)
target_link_libraries(celer PUBLIC celer_liburing)

if (CELER_WITH_DPDK)
  target_compile_definitions(celer PUBLIC CELER_WITH_DPDK)
  # Future DPDK backend wiring goes here:
  #   target_sources(celer PRIVATE src/io/dpdk_backend.cpp)
  #   target_link_libraries(celer PUBLIC dpdk)
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(celer PRIVATE -Wall -Wextra -Wpedantic)
  if (CELER_ENABLE_WERROR)
    target_compile_options(celer PRIVATE -Werror)
  endif()
endif()

# Optional RPC framework (built on celer's public net primitives). Separate target
# so apps that don't need RPC don't link it.
add_library(celer_rpc
  src/rpc/rpc.cpp
)
add_library(celer::rpc ALIAS celer_rpc)
target_link_libraries(celer_rpc PUBLIC celer::core)

if (CELER_BUILD_EXAMPLES)
  add_executable(celer_echo
    example/echo_server.cpp
  )
  target_link_libraries(celer_echo PRIVATE celer::core)

  add_executable(rpc_echo_server
    example/rpc_echo_server.cpp
  )
  target_link_libraries(rpc_echo_server PRIVATE celer::rpc)

  add_executable(rpc_bench
    example/rpc_bench.cpp
  )
  target_link_libraries(rpc_bench PRIVATE celer::rpc)
endif()
