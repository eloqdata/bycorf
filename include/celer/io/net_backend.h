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

#ifndef CELER_IO_NET_BACKEND_H_
#define CELER_IO_NET_BACKEND_H_

// The network backend is chosen at compile time, so every call through
// `backend_` inlines with zero abstraction overhead (no virtual dispatch).
// io_uring is the default. DPDK exposes the same interface as IoUringBackend
// (Init/Shutdown/Submit/Poll/Wait/SubmitSend/SubmitAcceptMultishot/
// StartRecvMultishot/ViewRecvBuffer/ReleaseRecvBuffer/WakeRemote/WakeSelf/...).
#ifdef CELER_WITH_DPDK
#include "celer/io/dpdk_backend.h"
namespace celer {
using NetBackend = DpdkBackend;
}
#else
#include "celer/io/io_uring_backend.h"

namespace celer {
using NetBackend = IoUringBackend;
}  // namespace celer
#endif

#endif  // CELER_IO_NET_BACKEND_H_
