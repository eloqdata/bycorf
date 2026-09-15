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
find_program(CELER_FREEBSD_CC NAMES clang-18 clang REQUIRED)
# Use the application's target even when the private kernel compiler is a
# different Clang binary. Host uname cannot select headers in a cross build.
set(CELER_FREEBSD_TARGET "${CMAKE_C_COMPILER_TARGET}")
if (NOT CELER_FREEBSD_TARGET)
  execute_process(COMMAND "${CMAKE_C_COMPILER}" -dumpmachine
    OUTPUT_VARIABLE CELER_FREEBSD_TARGET OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
endif()
set(CELER_FREEBSD_ARCHIVE "${CMAKE_CURRENT_BINARY_DIR}/freebsd/libceler_freebsd.a")
file(GLOB_RECURSE CELER_FREEBSD_INPUTS CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/freebsd/*"
  "${CMAKE_CURRENT_SOURCE_DIR}/src/io/freebsd/*")
add_custom_command(OUTPUT "${CELER_FREEBSD_ARCHIVE}"
  COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/cmake/build_freebsd.py"
    --source "${CMAKE_CURRENT_SOURCE_DIR}"
    --output "${CMAKE_CURRENT_BINARY_DIR}/freebsd" --cc "${CELER_FREEBSD_CC}"
    --target "${CELER_FREEBSD_TARGET}"
  DEPENDS ${CELER_FREEBSD_INPUTS} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/build_freebsd.py"
  COMMENT "Building the private FreeBSD IPv4/TCP stack" VERBATIM)
add_custom_target(celer_freebsd_build DEPENDS "${CELER_FREEBSD_ARCHIVE}")
add_library(celer_freebsd STATIC IMPORTED GLOBAL)
set_target_properties(celer_freebsd PROPERTIES IMPORTED_LOCATION "${CELER_FREEBSD_ARCHIVE}")
add_dependencies(celer_freebsd celer_freebsd_build)
target_link_libraries(celer PRIVATE celer_freebsd)
