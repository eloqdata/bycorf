// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#if BYCORF_ENABLE_RDMA

#include "absl/status/statusor.h"
#include "bycorf/base/local_shared_ptr.h"

namespace bycorf {
class Worker;
namespace rdma {
class RcConnection;
class RcListener;
namespace detail {
class Engine;
// Used only by the private worker facade. Shutdown has two phases because
// io_uring drops outstanding tags without callbacks when the ring is destroyed.
absl::StatusOr<LocalSharedPtr<Engine>> CreateEngine(Worker& worker);
void BeginShutdown(const LocalSharedPtr<Engine>& engine);
void AfterRingShutdown(const LocalSharedPtr<Engine>& engine);
}  // namespace detail
}  // namespace rdma
}  // namespace bycorf
#endif
