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

find_package(Python3 REQUIRED COMPONENTS Interpreter)
find_program(BYCORF_FREEBSD_CC NAMES clang-18 clang REQUIRED)
find_program(BYCORF_FREEBSD_PATCH NAMES patch REQUIRED)
# Use the application's target even when the private kernel compiler is a
# different Clang binary. Host uname cannot select headers in a cross build.
set(BYCORF_FREEBSD_TARGET "${CMAKE_C_COMPILER_TARGET}")
if (NOT BYCORF_FREEBSD_TARGET)
  execute_process(COMMAND "${CMAKE_C_COMPILER}" -dumpmachine
    OUTPUT_VARIABLE BYCORF_FREEBSD_TARGET OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
endif()
set(BYCORF_FREEBSD_ARCHIVE "${CMAKE_CURRENT_BINARY_DIR}/freebsd/libbycorf_freebsd.a")
file(GLOB_RECURSE BYCORF_FREEBSD_INPUTS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/freebsd/*"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/io/freebsd/*")
add_custom_command(OUTPUT "${BYCORF_FREEBSD_ARCHIVE}"
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/build_freebsd.py"
    --source "${CMAKE_CURRENT_SOURCE_DIR}"
    --output "${CMAKE_CURRENT_BINARY_DIR}/freebsd" --cc "${BYCORF_FREEBSD_CC}"
    --patch "${BYCORF_FREEBSD_PATCH}"
    --target "${BYCORF_FREEBSD_TARGET}"
    --max-workers "${BYCORF_DPDK_MAX_WORKERS}"
  DEPENDS ${BYCORF_FREEBSD_INPUTS} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/build_freebsd.py"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/patches/freebsd-tcp-iss-retirement.patch"
  COMMENT "Building the private FreeBSD IPv4/TCP stack" VERBATIM)
add_custom_target(bycorf_freebsd_build DEPENDS "${BYCORF_FREEBSD_ARCHIVE}")
add_library(bycorf_freebsd STATIC IMPORTED GLOBAL)
set_target_properties(bycorf_freebsd PROPERTIES IMPORTED_LOCATION "${BYCORF_FREEBSD_ARCHIVE}")
add_dependencies(bycorf_freebsd bycorf_freebsd_build)
target_link_libraries(bycorf PRIVATE bycorf_freebsd)
