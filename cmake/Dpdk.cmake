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

# SPDK and the network backend use this single DPDK installation. SPDK's
# nested DPDK checkout is deliberately not part of the build graph.
set(BYCORF_DPDK_PREFIX "" CACHE PATH "Use an existing build of the pinned DPDK")
set(BYCORF_DPDK_DRIVERS "net/tap,net/ring,net/virtio" CACHE STRING
    "DPDK drivers to compile for the prototype")
set(BYCORF_DPDK_MAX_WORKERS "128" CACHE STRING
    "Maximum DPDK network workers supported by this build")
# EAL's initializer keeps one registration in addition to the existing Bycorf
# worker threads. SPDK's pinned configure supports at most 1024 lcore slots.
if (NOT BYCORF_DPDK_MAX_WORKERS MATCHES "^[1-9][0-9]*$" OR
    BYCORF_DPDK_MAX_WORKERS GREATER 1023)
  message(FATAL_ERROR "BYCORF_DPDK_MAX_WORKERS must be an integer in 1..1023")
endif()
math(EXPR BYCORF_DPDK_REQUIRED_LCORES "${BYCORF_DPDK_MAX_WORKERS} + 1")
target_compile_definitions(bycorf PRIVATE
  BYCORF_DPDK_MAX_WORKERS=${BYCORF_DPDK_MAX_WORKERS})
if (NOT BYCORF_DPDK_PREFIX)
  find_program(BYCORF_MESON_EXECUTABLE meson REQUIRED)
  find_program(BYCORF_NINJA_EXECUTABLE ninja REQUIRED)
  set(BYCORF_DPDK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/dpdk-install")
  set(BYCORF_DPDK_BUILD "${CMAKE_CURRENT_BINARY_DIR}/dpdk-build")
  set(BYCORF_DPDK_RECONFIGURE "")
  if (EXISTS "${BYCORF_DPDK_BUILD}/build.ninja")
    set(BYCORF_DPDK_RECONFIGURE --reconfigure)
  endif()
  # Honor the selected project compiler even when an embedding build sets CC
  # or CXX for another configure-time dependency such as SPDK.
  execute_process(COMMAND "${CMAKE_COMMAND}" -E env
      "CC=${CMAKE_C_COMPILER}" "CXX=${CMAKE_CXX_COMPILER}"
      "${BYCORF_MESON_EXECUTABLE}" setup
      ${BYCORF_DPDK_RECONFIGURE}
      "${BYCORF_DPDK_BUILD}" "${CMAKE_CURRENT_SOURCE_DIR}/third_party/dpdk"
      "--prefix=${BYCORF_DPDK_PREFIX}" --libdir=lib --buildtype=release
      -Ddefault_library=static -Dexamples= -Dtests=false -Ddisable_apps=*
      "-Denable_drivers=${BYCORF_DPDK_DRIVERS}"
      "-Dmax_lcores=${BYCORF_DPDK_REQUIRED_LCORES}" -Dplatform=generic
      COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${BYCORF_NINJA_EXECUTABLE}" -C "${BYCORF_DPDK_BUILD}" -j4
    COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${BYCORF_NINJA_EXECUTABLE}" -C "${BYCORF_DPDK_BUILD}" install
    COMMAND_ERROR_IS_FATAL ANY)
endif()
# RTE_MAX_LCORE affects public structure layouts. Never override the installed
# header to pretend an external dependency has more slots than its libraries.
file(STRINGS "${BYCORF_DPDK_PREFIX}/include/rte_build_config.h"
  BYCORF_DPDK_LCORE_DEFINE REGEX "^#define[ \t]+RTE_MAX_LCORE[ \t]+[0-9]+")
string(REGEX REPLACE "^#define[ \t]+RTE_MAX_LCORE[ \t]+([0-9]+).*" "\\1"
  BYCORF_DPDK_LCORE_CAPACITY "${BYCORF_DPDK_LCORE_DEFINE}")
if (NOT BYCORF_DPDK_LCORE_CAPACITY MATCHES "^[1-9][0-9]*$")
  message(FATAL_ERROR "Cannot read RTE_MAX_LCORE from BYCORF_DPDK_PREFIX")
endif()
if (BYCORF_DPDK_LCORE_CAPACITY LESS BYCORF_DPDK_REQUIRED_LCORES)
  message(FATAL_ERROR
    "DPDK prefix has ${BYCORF_DPDK_LCORE_CAPACITY} lcore slots; "
    "BYCORF_DPDK_MAX_WORKERS=${BYCORF_DPDK_MAX_WORKERS} requires "
    "${BYCORF_DPDK_REQUIRED_LCORES}. Rebuild DPDK or lower BYCORF_DPDK_MAX_WORKERS.")
endif()
if (BYCORF_DPDK_LCORE_CAPACITY GREATER 1024)
  message(FATAL_ERROR "The pinned SPDK supports at most 1024 DPDK lcore slots")
endif()
if (NOT EXISTS "${BYCORF_DPDK_PREFIX}/lib/pkgconfig/libdpdk.pc")
  message(FATAL_ERROR "BYCORF_DPDK_PREFIX does not contain lib/pkgconfig/libdpdk.pc")
endif()
