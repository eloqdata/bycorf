// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#if BYCORF_ENABLE_RDMA
#include <memory>

#include "absl/status/statusor.h"

namespace bycorf {
class Worker;
namespace rdma {
class RcConnection;
class RcListener;
namespace detail {
class Engine;
// Used only by the private worker facade. Shutdown has two phases because
// io_uring drops outstanding tags without callbacks when the ring is destroyed.
absl::StatusOr<std::shared_ptr<Engine>> CreateEngine(Worker& worker);
void BeginShutdown(const std::shared_ptr<Engine>& engine);
void AfterRingShutdown(const std::shared_ptr<Engine>& engine);
}  // namespace detail
}  // namespace rdma
}  // namespace bycorf
#endif
