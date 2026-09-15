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
set(CELER_DPDK_PREFIX "" CACHE PATH "Use an existing build of the pinned DPDK")
set(CELER_DPDK_DRIVERS "net/tap,net/ring,net/virtio" CACHE STRING
    "DPDK drivers to compile for the prototype")
set(CELER_DPDK_MAX_WORKERS "128" CACHE STRING
    "Maximum DPDK network workers supported by this build")
# EAL's initializer keeps one registration in addition to the existing Celer
# worker threads. SPDK's pinned configure supports at most 1024 lcore slots.
if (NOT CELER_DPDK_MAX_WORKERS MATCHES "^[1-9][0-9]*$" OR
    CELER_DPDK_MAX_WORKERS GREATER 1023)
  message(FATAL_ERROR "CELER_DPDK_MAX_WORKERS must be an integer in 1..1023")
endif()
math(EXPR CELER_DPDK_REQUIRED_LCORES "${CELER_DPDK_MAX_WORKERS} + 1")
target_compile_definitions(celer PRIVATE
  CELER_DPDK_MAX_WORKERS=${CELER_DPDK_MAX_WORKERS})
if (NOT CELER_DPDK_PREFIX)
  find_program(CELER_MESON_EXECUTABLE meson REQUIRED)
  find_program(CELER_NINJA_EXECUTABLE ninja REQUIRED)
  set(CELER_DPDK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/dpdk-install")
  set(CELER_DPDK_BUILD "${CMAKE_CURRENT_BINARY_DIR}/dpdk-build")
  set(CELER_DPDK_RECONFIGURE "")
  if (EXISTS "${CELER_DPDK_BUILD}/build.ninja")
    set(CELER_DPDK_RECONFIGURE --reconfigure)
  endif()
  execute_process(COMMAND "${CELER_MESON_EXECUTABLE}" setup
      ${CELER_DPDK_RECONFIGURE}
      "${CELER_DPDK_BUILD}" "${CMAKE_CURRENT_SOURCE_DIR}/third_party/dpdk"
      "--prefix=${CELER_DPDK_PREFIX}" --libdir=lib --buildtype=release
      -Ddefault_library=static -Dexamples= -Dtests=false -Ddisable_apps=*
      "-Denable_drivers=${CELER_DPDK_DRIVERS}"
      "-Dmax_lcores=${CELER_DPDK_REQUIRED_LCORES}" -Dplatform=generic
      COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${CELER_NINJA_EXECUTABLE}" -C "${CELER_DPDK_BUILD}" -j4
    COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${CELER_NINJA_EXECUTABLE}" -C "${CELER_DPDK_BUILD}" install
    COMMAND_ERROR_IS_FATAL ANY)
endif()
# RTE_MAX_LCORE affects public structure layouts. Never override the installed
# header to pretend an external dependency has more slots than its libraries.
file(STRINGS "${CELER_DPDK_PREFIX}/include/rte_build_config.h"
  CELER_DPDK_LCORE_DEFINE REGEX "^#define[ \t]+RTE_MAX_LCORE[ \t]+[0-9]+")
string(REGEX REPLACE "^#define[ \t]+RTE_MAX_LCORE[ \t]+([0-9]+).*" "\\1"
  CELER_DPDK_LCORE_CAPACITY "${CELER_DPDK_LCORE_DEFINE}")
if (NOT CELER_DPDK_LCORE_CAPACITY MATCHES "^[1-9][0-9]*$")
  message(FATAL_ERROR "Cannot read RTE_MAX_LCORE from CELER_DPDK_PREFIX")
endif()
if (CELER_DPDK_LCORE_CAPACITY LESS CELER_DPDK_REQUIRED_LCORES)
  message(FATAL_ERROR
    "DPDK prefix has ${CELER_DPDK_LCORE_CAPACITY} lcore slots; "
    "CELER_DPDK_MAX_WORKERS=${CELER_DPDK_MAX_WORKERS} requires "
    "${CELER_DPDK_REQUIRED_LCORES}. Rebuild DPDK or lower CELER_DPDK_MAX_WORKERS.")
endif()
if (CELER_DPDK_LCORE_CAPACITY GREATER 1024)
  message(FATAL_ERROR "The pinned SPDK supports at most 1024 DPDK lcore slots")
endif()
if (NOT EXISTS "${CELER_DPDK_PREFIX}/lib/pkgconfig/libdpdk.pc")
  message(FATAL_ERROR "CELER_DPDK_PREFIX does not contain lib/pkgconfig/libdpdk.pc")
endif()
