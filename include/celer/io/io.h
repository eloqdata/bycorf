/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

namespace celer::io {

// Marker header for the transport-agnostic I/O layer.
//
// This namespace is the future home for:
// - registered/fixed buffers
// - direct/fixed file descriptors
// - file and storage handles
// - common I/O ownership utilities shared by net/fs modules
//
// The current codebase is still network-first, so this header intentionally
// stays minimal while the layering is established.

enum class Domain {
  kGeneric,
  kNetwork,
  kFile,
};

}  // namespace celer::io
