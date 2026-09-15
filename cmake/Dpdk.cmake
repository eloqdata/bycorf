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
      "-Denable_drivers=${CELER_DPDK_DRIVERS}" -Dmax_lcores=16 -Dplatform=generic
      COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${CELER_NINJA_EXECUTABLE}" -C "${CELER_DPDK_BUILD}" -j4
    COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${CELER_NINJA_EXECUTABLE}" -C "${CELER_DPDK_BUILD}" install
    COMMAND_ERROR_IS_FATAL ANY)
endif()
if (NOT EXISTS "${CELER_DPDK_PREFIX}/lib/pkgconfig/libdpdk.pc")
  message(FATAL_ERROR "CELER_DPDK_PREFIX does not contain lib/pkgconfig/libdpdk.pc")
endif()
