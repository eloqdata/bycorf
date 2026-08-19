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

#ifndef CELER_NET_SERVICE_H_
#define CELER_NET_SERVICE_H_

#include <span>
#include <string>

#include "absl/status/statusor.h"
#include "celer/runtime/task.h"

namespace celer {

class Worker;

// Host-level binding config handed to a service when it starts on a worker.
struct ServiceContext {
  std::span<const std::string> bind_addresses_;
  bool reuse_port_ = false;
};

// A unit of work hosted by a Server across all workers. The Server owns the
// worker threads; a Service only describes what to run on each of them. This is
// transport-agnostic: a TCP service runs an accept loop, a future UDP service a
// datagram loop — the Server neither knows nor cares which. None of these calls
// are on the per-request hot path (Prepare/Stop fire once per service, Run once
// per worker, at startup/shutdown).
class Service {
 public:
  virtual ~Service() = default;

  // Called once on the Server's thread before any worker starts, so the service
  // can size its per-worker state for `thread_count` workers.
  virtual void Prepare(unsigned thread_count) = 0;

  // Spawned once on each worker (runs on that worker's thread). Sets up its
  // sockets and serves until the worker stops, then returns.
  virtual Task<absl::Status> Run(Worker& worker, ServiceContext ctx) = 0;

  // Called on the Server's thread at shutdown (before the workers are stopped)
  // to close the service's sockets so its Run loops unblock. Must be
  // thread-safe.
  virtual void Stop() noexcept = 0;
};

}  // namespace celer

#endif  // CELER_NET_SERVICE_H_
