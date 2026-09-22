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

project(bycorf VERSION 0.1.0 LANGUAGES C CXX)

find_package(OpenSSL REQUIRED)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BYCORF_BUILD_EXAMPLES "Build example applications" ON)
# Standalone builds expose CTest by default. Embedders own their test graph
# and opt in explicitly, so adding Bycorf does not register tests for the parent.
set(BYCORF_BUILD_TESTS_DEFAULT OFF)
if (CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
  include(CTest)
  set(BYCORF_BUILD_TESTS_DEFAULT "${BUILD_TESTING}")
endif()
option(BYCORF_BUILD_TESTS "Build Bycorf regression tests"
  ${BYCORF_BUILD_TESTS_DEFAULT})
option(BYCORF_ENABLE_WERROR "Treat warnings as errors" OFF)
# One dependency boundary includes both accelerators; applications still select
# network and storage independently at startup. DPDK uses private FreeBSD TCP.
option(BYCORF_KERNEL_BYPASS
  "Include DPDK networking and SPDK storage (selected independently at startup)"
  OFF)
option(BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE
  "Enable detailed cross-core queue and wake latency tracing" OFF)

set(BYCORF_CLANG_FORMAT_REQUIRED_VERSION "23.1.1")
string(REPLACE "." "\\." BYCORF_CLANG_FORMAT_VERSION_REGEX
  "${BYCORF_CLANG_FORMAT_REQUIRED_VERSION}")
find_program(BYCORF_CLANG_FORMAT NAMES clang-format-23 clang-format)
if (BYCORF_CLANG_FORMAT)
  execute_process(
    COMMAND ${BYCORF_CLANG_FORMAT} --version
    RESULT_VARIABLE BYCORF_CLANG_FORMAT_VERSION_RESULT
    OUTPUT_VARIABLE BYCORF_CLANG_FORMAT_VERSION_OUTPUT
    ERROR_VARIABLE BYCORF_CLANG_FORMAT_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE
  )
  # Repository-wide formatting must be deterministic across developer
  # machines; other releases remain unavailable to the format targets.
  if (NOT BYCORF_CLANG_FORMAT_VERSION_RESULT EQUAL 0 OR
      NOT BYCORF_CLANG_FORMAT_VERSION_OUTPUT MATCHES
        "clang-format version ${BYCORF_CLANG_FORMAT_VERSION_REGEX}([^0-9.]|$)")
    unset(BYCORF_CLANG_FORMAT CACHE)
    set(BYCORF_CLANG_FORMAT "")
  endif()
endif()
file(GLOB_RECURSE BYCORF_FORMAT_FILES CONFIGURE_DEPENDS
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c
  ${CMAKE_CURRENT_SOURCE_DIR}/example/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/bycorf/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/src/*.h
  ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp
)
list(FILTER BYCORF_FORMAT_FILES EXCLUDE REGEX "/runtime/concurrentqueue\\.h$")
if (BYCORF_CLANG_FORMAT)
  add_custom_target(bycorf-format
    COMMAND ${BYCORF_CLANG_FORMAT} -i ${BYCORF_FORMAT_FILES}
    COMMENT "Formatting bycorf sources"
    VERBATIM
  )
  add_custom_target(bycorf-format-check
    COMMAND ${BYCORF_CLANG_FORMAT} --dry-run --Werror ${BYCORF_FORMAT_FILES}
    COMMENT "Checking bycorf source formatting"
    VERBATIM
  )
  if (CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    add_custom_target(format DEPENDS bycorf-format)
    add_custom_target(format-check DEPENDS bycorf-format-check)
  endif()
else()
  message(STATUS
    "clang-format ${BYCORF_CLANG_FORMAT_REQUIRED_VERSION} not found; "
    "bycorf format targets are disabled")
endif()

set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
set(ABSL_BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BYCORF_ABSEIL_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil")
if (NOT EXISTS "${BYCORF_ABSEIL_SOURCE_DIR}/CMakeLists.txt")
  message(FATAL_ERROR
    "Abseil submodule not found; run git submodule update --init --recursive")
endif()
if (NOT TARGET absl::base)
  add_subdirectory(
    "${BYCORF_ABSEIL_SOURCE_DIR}"
    "${CMAKE_CURRENT_BINARY_DIR}/third_party/abseil"
    EXCLUDE_FROM_ALL
  )
endif()

set(BYCORF_LIBURING_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/liburing")
if (NOT EXISTS "${BYCORF_LIBURING_SOURCE_DIR}/configure")
  message(FATAL_ERROR
    "liburing submodule not found; run git submodule update --init --recursive")
endif()

include(ExternalProject)
find_program(BYCORF_MAKE_EXECUTABLE NAMES gmake make REQUIRED)

set(BYCORF_LIBURING_JOBSERVER_ARGS)
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.28")
  list(APPEND BYCORF_LIBURING_JOBSERVER_ARGS BUILD_JOB_SERVER_AWARE TRUE)
endif()

set(BYCORF_LIBURING_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/liburing")
set(BYCORF_LIBURING_ARCHIVE "${BYCORF_LIBURING_BINARY_DIR}/src/liburing.a")
# CMake validates imported include paths during generation, before liburing's
# configure script has created its generated-header directory.
file(MAKE_DIRECTORY "${BYCORF_LIBURING_BINARY_DIR}/src/include")

ExternalProject_Add(bycorf_liburing_build
  SOURCE_DIR "${BYCORF_LIBURING_SOURCE_DIR}"
  BINARY_DIR "${BYCORF_LIBURING_BINARY_DIR}"
  DOWNLOAD_COMMAND ""
  UPDATE_COMMAND ""
  PATCH_COMMAND ""
  CONFIGURE_COMMAND
    "${BYCORF_LIBURING_SOURCE_DIR}/configure"
      "--cc=${CMAKE_C_COMPILER}"
      "--cxx=${CMAKE_CXX_COMPILER}"
  BUILD_COMMAND
    "${BYCORF_MAKE_EXECUTABLE}" -C "${BYCORF_LIBURING_BINARY_DIR}/src"
      liburing.a ENABLE_SHARED=0
  INSTALL_COMMAND ""
  BUILD_BYPRODUCTS "${BYCORF_LIBURING_ARCHIVE}"
  ${BYCORF_LIBURING_JOBSERVER_ARGS}
)

add_library(bycorf_liburing STATIC IMPORTED GLOBAL)
set_target_properties(bycorf_liburing PROPERTIES
  IMPORTED_LOCATION "${BYCORF_LIBURING_ARCHIVE}"
  INTERFACE_INCLUDE_DIRECTORIES
    "${BYCORF_LIBURING_BINARY_DIR}/src/include;${BYCORF_LIBURING_SOURCE_DIR}/src/include"
)
add_dependencies(bycorf_liburing bycorf_liburing_build)

add_library(bycorf
  src/io/io.cpp
  src/io/storage.cpp
  src/io/backend_options.cpp
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

target_compile_definitions(bycorf PUBLIC
  # Public headers and consumers must agree on the backend-dependent layouts.
  # Export one numeric switch for both capabilities, including zero when off.
  BYCORF_KERNEL_BYPASS=$<BOOL:${BYCORF_KERNEL_BYPASS}>
  BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE=$<BOOL:${BYCORF_ENABLE_CROSS_CORE_LATENCY_TRACE}>
)

if (BYCORF_KERNEL_BYPASS)
  include(cmake/Dpdk.cmake)
  set(BYCORF_SPDK_SOURCE_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/spdk")
  if (NOT EXISTS "${BYCORF_SPDK_SOURCE_DIR}/configure")
    message(FATAL_ERROR
      "SPDK submodules are missing; run git submodule update --init --recursive")
  endif()
  find_program(BYCORF_PKG_CONFIG_EXECUTABLE NAMES pkg-config REQUIRED)
  find_package(OpenSSL REQUIRED)
  set(BYCORF_SPDK_CONFIGURED_DPDK "")
  set(BYCORF_SPDK_CONFIGURED_LCORES "")
  if (EXISTS "${BYCORF_SPDK_SOURCE_DIR}/mk/config.mk")
    file(STRINGS "${BYCORF_SPDK_SOURCE_DIR}/mk/config.mk" BYCORF_SPDK_CONFIGURED_DPDK
      REGEX "^CONFIG_DPDK_DIR")
    string(REGEX REPLACE "^[^=]*=" "" BYCORF_SPDK_CONFIGURED_DPDK "${BYCORF_SPDK_CONFIGURED_DPDK}")
    file(STRINGS "${BYCORF_SPDK_SOURCE_DIR}/mk/config.mk" BYCORF_SPDK_CONFIGURED_LCORES
      REGEX "^CONFIG_MAX_LCORES")
    string(REGEX REPLACE "^[^=]*=" "" BYCORF_SPDK_CONFIGURED_LCORES "${BYCORF_SPDK_CONFIGURED_LCORES}")
  endif()
  # A capacity change can rebuild DPDK at the same prefix. Reconfigure SPDK
  # too, so its own capacity and all rebuilt consumers use compatible headers.
  if (NOT BYCORF_SPDK_CONFIGURED_DPDK STREQUAL BYCORF_DPDK_PREFIX OR
      NOT BYCORF_SPDK_CONFIGURED_LCORES STREQUAL BYCORF_DPDK_LCORE_CAPACITY)
    execute_process(
      COMMAND "${BYCORF_SPDK_SOURCE_DIR}/configure"
        --disable-tests --disable-unit-tests --disable-examples --disable-apps
        --without-crypto --without-vhost --without-virtio --without-nvme-cuse
        --without-fsdev --without-shared
        "--max-lcores=${BYCORF_DPDK_LCORE_CAPACITY}"
        "--with-dpdk=${BYCORF_DPDK_PREFIX}"
      WORKING_DIRECTORY "${BYCORF_SPDK_SOURCE_DIR}"
      RESULT_VARIABLE BYCORF_SPDK_CONFIGURE_RESULT
    )
    if (NOT BYCORF_SPDK_CONFIGURE_RESULT EQUAL 0)
      message(FATAL_ERROR "SPDK configure failed")
    endif()
  endif()
  execute_process(
    COMMAND "${BYCORF_MAKE_EXECUTABLE}" -j8
    WORKING_DIRECTORY "${BYCORF_SPDK_SOURCE_DIR}"
    RESULT_VARIABLE BYCORF_SPDK_BUILD_RESULT
  )
  if (NOT BYCORF_SPDK_BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR
      "SPDK build failed; run third_party/spdk/scripts/pkgdep.sh or install its documented dependencies")
  endif()
  set(BYCORF_SPDK_PKG_CONFIG_PATH
    "${BYCORF_SPDK_SOURCE_DIR}/build/lib/pkgconfig:${BYCORF_DPDK_PREFIX}/lib/pkgconfig")
  # DPDK's inline headers require the CPU features of its build (for example
  # SSSE3 on x86). Include paths alone are insufficient when the application
  # does not add native/aggressive optimization flags of its own.
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
      "PKG_CONFIG_PATH=${BYCORF_SPDK_PKG_CONFIG_PATH}"
      "${BYCORF_PKG_CONFIG_EXECUTABLE}" --cflags-only-other libdpdk
    OUTPUT_VARIABLE BYCORF_DPDK_COMPILE_FLAGS_RAW OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY
  )
  separate_arguments(BYCORF_DPDK_COMPILE_FLAGS NATIVE_COMMAND
    "${BYCORF_DPDK_COMPILE_FLAGS_RAW}")
  target_compile_options(bycorf PRIVATE ${BYCORF_DPDK_COMPILE_FLAGS})
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
      "PKG_CONFIG_PATH=${BYCORF_SPDK_PKG_CONFIG_PATH}"
      "${BYCORF_PKG_CONFIG_EXECUTABLE}" --libs --static
      spdk_nvme spdk_env_dpdk
    OUTPUT_VARIABLE BYCORF_SPDK_LINK_FLAGS_RAW
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE BYCORF_SPDK_PKG_CONFIG_RESULT
  )
  if (NOT BYCORF_SPDK_PKG_CONFIG_RESULT EQUAL 0)
    message(FATAL_ERROR "pkg-config could not resolve static SPDK libraries")
  endif()
  separate_arguments(BYCORF_SPDK_LINK_FLAGS NATIVE_COMMAND
    "${BYCORF_SPDK_LINK_FLAGS_RAW}")
  list(FILTER BYCORF_SPDK_LINK_FLAGS EXCLUDE REGEX
    "^(-pthread|-lm|-ldl|-lnuma)$")
  # SPDK's pkg-config selects only its EAL/storage dependencies, omitting
  # network PMD constructors. Replace that subset with one complete DPDK
  # archive list; linking both under whole-archive duplicates EAL symbols.
  execute_process(COMMAND "${CMAKE_COMMAND}" -E env
    "PKG_CONFIG_PATH=${BYCORF_SPDK_PKG_CONFIG_PATH}"
    "${BYCORF_PKG_CONFIG_EXECUTABLE}" --libs --static libdpdk
    OUTPUT_VARIABLE BYCORF_DPDK_FLAGS_RAW OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
  separate_arguments(BYCORF_DPDK_FLAGS NATIVE_COMMAND "${BYCORF_DPDK_FLAGS_RAW}")
  list(FILTER BYCORF_SPDK_LINK_FLAGS EXCLUDE REGEX "^-lrte_")
  set(BYCORF_DPDK_SYSTEM_FLAGS ${BYCORF_DPDK_FLAGS})
  list(FILTER BYCORF_DPDK_FLAGS INCLUDE REGEX "^-l:librte_.*\\.a$")
  list(FILTER BYCORF_DPDK_SYSTEM_FLAGS EXCLUDE REGEX "^(-l:?lib?rte_|-lrte_|-Wl,|-L)")
  list(APPEND BYCORF_SPDK_LINK_FLAGS ${BYCORF_DPDK_FLAGS})
  # spdk_nvme/spdk_env_dpdk do not propagate the ISA-L archives from
  # spdk_syslibs.pc. They provide CRC/XOR symbols used by libspdk_util, so
  # retain the non-`-l` archive entries without pulling OpenSSL and other
  # system libraries into --whole-archive.
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
      "PKG_CONFIG_PATH=${BYCORF_SPDK_PKG_CONFIG_PATH}"
      "${BYCORF_PKG_CONFIG_EXECUTABLE}" --libs-only-other --static
      spdk_syslibs
    OUTPUT_VARIABLE BYCORF_SPDK_SYSTEM_ARCHIVES_RAW
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE BYCORF_SPDK_SYSTEM_ARCHIVES_RESULT
  )
  if (NOT BYCORF_SPDK_SYSTEM_ARCHIVES_RESULT EQUAL 0)
    message(FATAL_ERROR "pkg-config could not resolve SPDK system archives")
  endif()
  separate_arguments(BYCORF_SPDK_SYSTEM_ARCHIVES NATIVE_COMMAND
    "${BYCORF_SPDK_SYSTEM_ARCHIVES_RAW}")
  list(FILTER BYCORF_SPDK_SYSTEM_ARCHIVES EXCLUDE REGEX "^-pthread$")
  include(cmake/Freebsd.cmake)
  target_sources(bycorf PRIVATE
    src/io/dpdk_environment.cpp src/io/spdk_storage.cpp
    src/io/dpdk_backend.cpp src/net/socket_ops.cpp)
  target_include_directories(bycorf SYSTEM PUBLIC
    "${BYCORF_SPDK_SOURCE_DIR}/include"
    "${BYCORF_SPDK_SOURCE_DIR}/build/include"
    "${BYCORF_DPDK_PREFIX}/include"
  )
  target_link_libraries(bycorf PUBLIC
    # SPDK and DPDK discover transports and bus drivers through static
    # constructors.  Keep every object from these archives; otherwise the
    # linker drops the PCIe transport because no ordinary symbol references it.
    "-Wl,--whole-archive"
    "-Wl,-Bstatic"
    ${BYCORF_SPDK_LINK_FLAGS}
    "-Wl,--no-whole-archive"
    # ISA-L exports a generic gf_mul symbol that OpenSSL also defines. Keep
    # these archives outside --whole-archive so only SPDK's referenced CRC/XOR
    # objects are selected.
    ${BYCORF_SPDK_SYSTEM_ARCHIVES}
    "-Wl,-Bdynamic"
    ${BYCORF_DPDK_SYSTEM_FLAGS}
    # CMake de-duplicates imported targets in a final consumer's link line.
    # Keep this explicit archive scan after SPDK: spdk_sock_posix can introduce
    # TLS references after an earlier OpenSSL::SSL occurrence was discarded.
    "-Wl,${OPENSSL_SSL_LIBRARY},${OPENSSL_CRYPTO_LIBRARY}"
    OpenSSL::SSL
    OpenSSL::Crypto
    uuid
    numa
    dl
    m
  )
endif()

add_library(bycorf::bycorf ALIAS bycorf)
add_library(bycorf::core ALIAS bycorf)
add_library(bycorf::io ALIAS bycorf)

target_include_directories(bycorf
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party  # vendored header-only spdlog
)
target_include_directories(bycorf SYSTEM PUBLIC
  ${BYCORF_ABSEIL_SOURCE_DIR}
)

target_compile_features(bycorf PUBLIC cxx_std_23)
target_link_libraries(bycorf PUBLIC
  bycorf_liburing
  absl::base
  absl::status
  absl::statusor
)
target_link_libraries(bycorf PRIVATE OpenSSL::SSL OpenSSL::Crypto
  absl::flat_hash_map)

# GCC lowers symmetric coroutine transfers to growing native calls unless
# sibling-call optimization is enabled, including at -O0. Task is a public
# header: consumers must compile their own coroutine bodies with this option.
# Keep other Debug optimizations, assertions and debug information unchanged.
target_compile_options(bycorf PUBLIC
  $<$<COMPILE_LANG_AND_ID:CXX,GNU>:-foptimize-sibling-calls>)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  target_compile_options(bycorf PRIVATE -Wall -Wextra -Wpedantic)
  if (BYCORF_ENABLE_WERROR)
    target_compile_options(bycorf PRIVATE -Werror)
  endif()
endif()

# Optional RPC framework (built on bycorf's public net primitives). Separate target
# so apps that don't need RPC don't link it.
add_library(bycorf_rpc
  src/rpc/rpc.cpp
)
add_library(bycorf::rpc ALIAS bycorf_rpc)
target_link_libraries(bycorf_rpc PUBLIC bycorf::core)

if (BYCORF_BUILD_EXAMPLES)
  add_executable(bycorf_echo
    example/echo_server.cpp
  )
  target_link_libraries(bycorf_echo PRIVATE bycorf::core)

  add_executable(rpc_echo_server
    example/rpc_echo_server.cpp
  )
  target_link_libraries(rpc_echo_server PRIVATE bycorf::rpc)

  add_executable(rpc_bench
    example/rpc_bench.cpp
  )
  target_link_libraries(rpc_bench PRIVATE bycorf::rpc)
endif()

if (BYCORF_BUILD_EXAMPLES OR BYCORF_BUILD_TESTS)
  add_executable(bycorf_task_transfer_check tests/task_transfer_check.cpp)
  target_include_directories(bycorf_task_transfer_check PRIVATE include)
  # Exercise the public compile policy with this build's actual optimization
  # level. Avoid linking Worker because this isolated check supplies its own
  # scheduling hooks to observe Task ownership and background classification.
  target_compile_options(bycorf_task_transfer_check PRIVATE
    $<TARGET_PROPERTY:bycorf,INTERFACE_COMPILE_OPTIONS>)

  add_executable(bycorf_connect_timer_check
    example/connect_timer_check.cpp
  )
  target_link_libraries(bycorf_connect_timer_check PRIVATE bycorf::core)

  add_executable(bycorf_connection_storage_check example/connection_storage_check.cpp)
  target_link_libraries(bycorf_connection_storage_check PRIVATE bycorf::core)

  add_executable(bycorf_backend_selection_check example/backend_selection_check.cpp)
  target_link_libraries(bycorf_backend_selection_check PRIVATE bycorf::core)

  add_executable(bycorf_backend_startup_check example/backend_startup_check.cpp)
  target_link_libraries(bycorf_backend_startup_check PRIVATE bycorf::core)

  if (BYCORF_KERNEL_BYPASS)
    add_executable(bycorf_backend_worker_check example/backend_worker_check.cpp)
    target_link_libraries(bycorf_backend_worker_check PRIVATE bycorf::core)

    find_package(Threads REQUIRED)
    add_executable(bycorf_freebsd_port_check tests/freebsd_port_check.cpp)
    target_link_libraries(bycorf_freebsd_port_check PRIVATE
      bycorf_freebsd Threads::Threads)
    add_executable(bycorf_freebsd_tcp_sequence_check tests/freebsd_tcp_sequence_check.cpp)
    target_link_libraries(bycorf_freebsd_tcp_sequence_check PRIVATE bycorf_freebsd)
  endif()
endif()

if (BYCORF_BUILD_TESTS)
  enable_testing()
  add_test(NAME bycorf_task_transfer_check COMMAND bycorf_task_transfer_check)
  set_tests_properties(bycorf_task_transfer_check PROPERTIES
    TIMEOUT 30 LABELS "runtime;task")
  if (BYCORF_KERNEL_BYPASS)
    add_executable(bycorf_freebsd_local_connect_check
      tests/freebsd_local_connect_check.cpp)
    target_link_libraries(bycorf_freebsd_local_connect_check PRIVATE bycorf_freebsd)
    add_test(NAME bycorf_freebsd_local_connect_check
      COMMAND bycorf_freebsd_local_connect_check)
    set_tests_properties(bycorf_freebsd_local_connect_check PROPERTIES
      TIMEOUT 15 LABELS "runtime;net;freebsd")
  endif()
  add_test(NAME bycorf_connect_timer_check COMMAND bycorf_connect_timer_check)
  add_test(NAME bycorf_connection_storage_check COMMAND bycorf_connection_storage_check)
  add_test(NAME bycorf_backend_selection_check COMMAND bycorf_backend_selection_check)
  set_tests_properties(bycorf_connection_storage_check bycorf_backend_selection_check
    PROPERTIES TIMEOUT 90 LABELS "runtime;net;io_uring")
  set_tests_properties(bycorf_connect_timer_check PROPERTIES
    TIMEOUT 90 LABELS "runtime;net;io_uring"
  )
endif()
