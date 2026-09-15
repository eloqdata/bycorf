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

find_package(OpenSSL REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(CELER_BUILD_EXAMPLES "Build example applications" ON)
option(CELER_ENABLE_WERROR "Treat warnings as errors" OFF)
# Selects the network backend at compile time (see io/net_backend.h). Default OFF
# = io_uring. DPDK uses a private FreeBSD IPv4/TCP stack.
option(CELER_WITH_DPDK "Build with the DPDK net backend instead of io_uring" OFF)
option(CELER_WITH_SPDK_STORAGE
  "Use SPDK for storage with either network backend" OFF)
option(CELER_ENABLE_CROSS_CORE_LATENCY_TRACE
  "Enable detailed cross-core queue and wake latency tracing" OFF)

set(CELER_CLANG_FORMAT_REQUIRED_VERSION "23.1.0")
string(REPLACE "." "\\." CELER_CLANG_FORMAT_VERSION_REGEX
  "${CELER_CLANG_FORMAT_REQUIRED_VERSION}")
find_program(CELER_CLANG_FORMAT NAMES clang-format-23 clang-format)
if (CELER_CLANG_FORMAT)
  execute_process(
    COMMAND ${CELER_CLANG_FORMAT} --version
    RESULT_VARIABLE CELER_CLANG_FORMAT_VERSION_RESULT
    OUTPUT_VARIABLE CELER_CLANG_FORMAT_VERSION_OUTPUT
    ERROR_VARIABLE CELER_CLANG_FORMAT_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE
  )
  # Repository-wide formatting must be deterministic across developer
  # machines; other releases remain unavailable to the format targets.
  if (NOT CELER_CLANG_FORMAT_VERSION_RESULT EQUAL 0 OR
      NOT CELER_CLANG_FORMAT_VERSION_OUTPUT MATCHES
        "clang-format version ${CELER_CLANG_FORMAT_VERSION_REGEX}([^0-9.]|$)")
    unset(CELER_CLANG_FORMAT CACHE)
    set(CELER_CLANG_FORMAT "")
  endif()
endif()
file(GLOB_RECURSE CELER_FORMAT_FILES CONFIGURE_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/example/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/celer/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
)
list(FILTER CELER_FORMAT_FILES EXCLUDE REGEX "/runtime/concurrentqueue\\.h$")
if (CELER_CLANG_FORMAT)
  add_custom_target(celer-format
    COMMAND ${CELER_CLANG_FORMAT} -i ${CELER_FORMAT_FILES}
    COMMENT "Formatting celer sources"
    VERBATIM
  )
  add_custom_target(celer-format-check
    COMMAND ${CELER_CLANG_FORMAT} --dry-run --Werror ${CELER_FORMAT_FILES}
    COMMENT "Checking celer source formatting"
    VERBATIM
  )
  if (CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    add_custom_target(format DEPENDS celer-format)
    add_custom_target(format-check DEPENDS celer-format-check)
  endif()
else()
  message(STATUS
    "clang-format ${CELER_CLANG_FORMAT_REQUIRED_VERSION} not found; "
    "celer format targets are disabled")
endif()

set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(CELER_ABSEIL_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil")
if (NOT EXISTS "${CELER_ABSEIL_SOURCE_DIR}/CMakeLists.txt")
  message(FATAL_ERROR
    "Abseil submodule not found; run git submodule update --init --recursive")
endif()
if (NOT TARGET absl::base)
  add_subdirectory(
    "${CELER_ABSEIL_SOURCE_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/third_party/abseil"
    EXCLUDE_FROM_ALL
  )
endif()

set(CELER_LIBURING_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/liburing")
if (NOT EXISTS "${CELER_LIBURING_SOURCE_DIR}/configure")
  message(FATAL_ERROR
    "liburing submodule not found; run git submodule update --init --recursive")
endif()

include(ExternalProject)
find_program(CELER_MAKE_EXECUTABLE NAMES gmake make REQUIRED)

set(CELER_LIBURING_JOBSERVER_ARGS)
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.28")
  list(APPEND CELER_LIBURING_JOBSERVER_ARGS BUILD_JOB_SERVER_AWARE TRUE)
endif()

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
  ${CELER_LIBURING_JOBSERVER_ARGS}
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
  src/net/http_service.cpp
  src/net/tcp_service.cpp
  src/runtime/foreign_executor.cpp
  src/runtime/worker.cpp
  src/runtime/runtime.cpp
  src/net/tcp_listener.cpp
  src/net/tcp_stream.cpp
  src/net/tls.cpp
)

target_compile_definitions(celer PUBLIC
  CELER_ENABLE_CROSS_CORE_LATENCY_TRACE=$<BOOL:${CELER_ENABLE_CROSS_CORE_LATENCY_TRACE}>
)

if (CELER_WITH_DPDK OR CELER_WITH_SPDK_STORAGE)
  include(cmake/Dpdk.cmake)
  set(CELER_SPDK_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/spdk")
  if (NOT EXISTS "${CELER_SPDK_SOURCE_DIR}/configure")
    message(FATAL_ERROR
      "SPDK submodules are missing; run git submodule update --init --recursive")
  endif()
  find_program(CELER_PKG_CONFIG_EXECUTABLE NAMES pkg-config REQUIRED)
  find_package(OpenSSL REQUIRED)
  set(CELER_SPDK_CONFIGURED_DPDK "")
  if (EXISTS "${CELER_SPDK_SOURCE_DIR}/mk/config.mk")
    file(STRINGS "${CELER_SPDK_SOURCE_DIR}/mk/config.mk" CELER_SPDK_CONFIGURED_DPDK
      REGEX "^CONFIG_DPDK_DIR")
    string(REGEX REPLACE "^[^=]*=" "" CELER_SPDK_CONFIGURED_DPDK "${CELER_SPDK_CONFIGURED_DPDK}")
  endif()
  # Reconfigure when switching between the nested build and our shared prefix.
  if (NOT CELER_SPDK_CONFIGURED_DPDK STREQUAL CELER_DPDK_PREFIX)
    execute_process(
      COMMAND "${CELER_SPDK_SOURCE_DIR}/configure"
        --disable-tests --disable-unit-tests --disable-examples --disable-apps
        --without-crypto --without-vhost --without-virtio --without-nvme-cuse
        --without-fsdev --without-shared --max-lcores=16
        "--with-dpdk=${CELER_DPDK_PREFIX}"
      WORKING_DIRECTORY "${CELER_SPDK_SOURCE_DIR}"
      RESULT_VARIABLE CELER_SPDK_CONFIGURE_RESULT
    )
    if (NOT CELER_SPDK_CONFIGURE_RESULT EQUAL 0)
      message(FATAL_ERROR "SPDK configure failed")
    endif()
  endif()
  execute_process(
    COMMAND "${CELER_MAKE_EXECUTABLE}" -j8
    WORKING_DIRECTORY "${CELER_SPDK_SOURCE_DIR}"
    RESULT_VARIABLE CELER_SPDK_BUILD_RESULT
  )
  if (NOT CELER_SPDK_BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR
      "SPDK build failed; run third_party/spdk/scripts/pkgdep.sh or install its documented dependencies")
  endif()
  set(CELER_SPDK_PKG_CONFIG_PATH
    "${CELER_SPDK_SOURCE_DIR}/build/lib/pkgconfig:${CELER_DPDK_PREFIX}/lib/pkgconfig")
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
      "PKG_CONFIG_PATH=${CELER_SPDK_PKG_CONFIG_PATH}"
      "${CELER_PKG_CONFIG_EXECUTABLE}" --libs --static
      spdk_nvme spdk_env_dpdk
    OUTPUT_VARIABLE CELER_SPDK_LINK_FLAGS_RAW
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE CELER_SPDK_PKG_CONFIG_RESULT
  )
  if (NOT CELER_SPDK_PKG_CONFIG_RESULT EQUAL 0)
    message(FATAL_ERROR "pkg-config could not resolve static SPDK libraries")
  endif()
  separate_arguments(CELER_SPDK_LINK_FLAGS NATIVE_COMMAND
    "${CELER_SPDK_LINK_FLAGS_RAW}")
  list(FILTER CELER_SPDK_LINK_FLAGS EXCLUDE REGEX
    "^(-pthread|-lm|-ldl|-lnuma)$")
  if (CELER_WITH_DPDK)
    # SPDK's pkg-config selects only its EAL/storage dependencies, omitting
    # network PMD constructors. Replace that subset with one complete DPDK
    # archive list; linking both under whole-archive duplicates EAL symbols.
    execute_process(COMMAND "${CMAKE_COMMAND}" -E env
      "PKG_CONFIG_PATH=${CELER_SPDK_PKG_CONFIG_PATH}"
      "${CELER_PKG_CONFIG_EXECUTABLE}" --libs --static libdpdk
      OUTPUT_VARIABLE CELER_DPDK_FLAGS_RAW OUTPUT_STRIP_TRAILING_WHITESPACE
      COMMAND_ERROR_IS_FATAL ANY)
    separate_arguments(CELER_DPDK_FLAGS NATIVE_COMMAND "${CELER_DPDK_FLAGS_RAW}")
    list(FILTER CELER_SPDK_LINK_FLAGS EXCLUDE REGEX "^-lrte_")
    set(CELER_DPDK_SYSTEM_FLAGS ${CELER_DPDK_FLAGS})
    list(FILTER CELER_DPDK_FLAGS INCLUDE REGEX "^-l:librte_.*\\.a$")
    list(FILTER CELER_DPDK_SYSTEM_FLAGS EXCLUDE REGEX "^(-l:?lib?rte_|-lrte_|-Wl,|-L)")
    list(APPEND CELER_SPDK_LINK_FLAGS ${CELER_DPDK_FLAGS})
  endif()
  # spdk_nvme/spdk_env_dpdk do not propagate the ISA-L archives from
  # spdk_syslibs.pc. They provide CRC/XOR symbols used by libspdk_util, so
  # retain the non-`-l` archive entries without pulling OpenSSL and other
  # system libraries into --whole-archive.
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
      "PKG_CONFIG_PATH=${CELER_SPDK_PKG_CONFIG_PATH}"
      "${CELER_PKG_CONFIG_EXECUTABLE}" --libs-only-other --static
      spdk_syslibs
    OUTPUT_VARIABLE CELER_SPDK_SYSTEM_ARCHIVES_RAW
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE CELER_SPDK_SYSTEM_ARCHIVES_RESULT
  )
  if (NOT CELER_SPDK_SYSTEM_ARCHIVES_RESULT EQUAL 0)
    message(FATAL_ERROR "pkg-config could not resolve SPDK system archives")
  endif()
  separate_arguments(CELER_SPDK_SYSTEM_ARCHIVES NATIVE_COMMAND
    "${CELER_SPDK_SYSTEM_ARCHIVES_RAW}")
  list(FILTER CELER_SPDK_SYSTEM_ARCHIVES EXCLUDE REGEX "^-pthread$")
  target_sources(celer PRIVATE src/io/dpdk_environment.cpp)
  if (CELER_WITH_SPDK_STORAGE)
    target_sources(celer PRIVATE src/io/spdk_storage.cpp)
    target_compile_definitions(celer PUBLIC CELER_WITH_SPDK_STORAGE=1)
  endif()
  target_include_directories(celer SYSTEM PUBLIC
    "${CELER_SPDK_SOURCE_DIR}/include"
    "${CELER_SPDK_SOURCE_DIR}/build/include"
    "${CELER_DPDK_PREFIX}/include"
  )
  target_link_libraries(celer PUBLIC
    # SPDK and DPDK discover transports and bus drivers through static
    # constructors.  Keep every object from these archives; otherwise the
    # linker drops the PCIe transport because no ordinary symbol references it.
    "-Wl,--whole-archive"
    "-Wl,-Bstatic"
    ${CELER_SPDK_LINK_FLAGS}
    "-Wl,--no-whole-archive"
    # ISA-L exports a generic gf_mul symbol that OpenSSL also defines. Keep
    # these archives outside --whole-archive so only SPDK's referenced CRC/XOR
    # objects are selected.
    ${CELER_SPDK_SYSTEM_ARCHIVES}
    "-Wl,-Bdynamic"
    ${CELER_DPDK_SYSTEM_FLAGS}
    OpenSSL::SSL
    OpenSSL::Crypto
    uuid
    numa
    dl
    m
  )
endif()

if (CELER_WITH_DPDK)
  include(cmake/Freebsd.cmake)
  target_compile_definitions(celer PUBLIC CELER_WITH_DPDK=1)
  target_sources(celer PRIVATE src/io/dpdk_backend.cpp src/net/socket_ops.cpp)
endif()

add_library(celer::celer ALIAS celer)
add_library(celer::core ALIAS celer)
add_library(celer::io ALIAS celer)

target_include_directories(celer
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party  # vendored header-only spdlog
)
target_include_directories(celer SYSTEM PUBLIC
  ${CELER_ABSEIL_SOURCE_DIR}
)

target_compile_features(celer PUBLIC cxx_std_23)
target_link_libraries(celer PUBLIC
  celer_liburing
  absl::base
  absl::status
  absl::statusor
)
target_link_libraries(celer PRIVATE OpenSSL::SSL OpenSSL::Crypto)

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

  add_executable(celer_connect_timer_check
    example/connect_timer_check.cpp
  )
  target_link_libraries(celer_connect_timer_check PRIVATE celer::core)

  add_executable(celer_backend_startup_check example/backend_startup_check.cpp)
  target_link_libraries(celer_backend_startup_check PRIVATE celer::core)

  if (CELER_WITH_DPDK)
    find_package(Threads REQUIRED)
    add_executable(celer_freebsd_port_check tests/freebsd_port_check.cpp)
    target_link_libraries(celer_freebsd_port_check PRIVATE
      celer_freebsd Threads::Threads)
  endif()
endif()
