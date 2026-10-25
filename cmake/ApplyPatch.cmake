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

# Apply a dependency patch without touching the index. Check the complete patch
# in either direction so reconfiguration is idempotent, while conflicts or a
# partially applied patch fail before compiling an unpatched dependency.
function(bycorf_apply_patch source_dir patch_file)
  find_package(Git REQUIRED)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${source_dir}" apply --reverse --check
      "${patch_file}"
    RESULT_VARIABLE reverse_result
    OUTPUT_QUIET ERROR_QUIET)
  if (reverse_result EQUAL 0)
    message(STATUS "Dependency patch already applied: ${patch_file}")
    return()
  endif()

  execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${source_dir}" apply --check "${patch_file}"
    RESULT_VARIABLE check_result
    OUTPUT_VARIABLE check_output ERROR_VARIABLE check_error)
  if (NOT check_result EQUAL 0)
    message(FATAL_ERROR
      "Cannot apply dependency patch ${patch_file} to ${source_dir}; "
      "it is neither applicable nor fully applied.\n"
      "${check_output}${check_error}")
  endif()

  execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${source_dir}" apply "${patch_file}"
    COMMAND_ERROR_IS_FATAL ANY)
  message(STATUS "Applied dependency patch: ${patch_file}")
endfunction()
