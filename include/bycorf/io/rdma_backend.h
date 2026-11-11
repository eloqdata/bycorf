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

#ifndef BYCORF_IO_RDMA_BACKEND_H_
#define BYCORF_IO_RDMA_BACKEND_H_

#if BYCORF_ENABLE_RDMA
#include "bycorf/io/io_uring_backend.h"
#include "bycorf/io/rdma_memory.h"
#include "bycorf/io/rdma_reactor.h"

namespace bycorf {
// Private CRTP facade: the owner supplies its existing KernelBackend(). Neither
// construction nor destruction downcasts or accesses devices/the shared ring.
// All calls run on the owning worker; external handles can outlive the facade.
template <typename Owner>
class RdmaBackend {
 public:
  // Explicit device selection; no default-device fallback. Opens lazily and
  // reuses a live worker-local PD for this context/device.
  absl::StatusOr<RdmaProtectionDomain> OpenRdmaDevice(std::string_view name) {
    return resources_.OpenDevice(name);
  }
  // For a CM-selected context, pass an owning shared_ptr (an aliasing pointer
  // may retain its CM owner). It must keep the exact context alive; wrapping a
  // borrowed pointer with a no-op deleter does not satisfy this contract.
  absl::StatusOr<RdmaProtectionDomain> AcquireRdmaDomain(
      const std::shared_ptr<ibv_context>& context) {
    return resources_.Domain(context);
  }
  // Rejects domains from another worker, invalid ranges and invalid access.
  // Caller owns memory; this API neither allocates nor grows backing buffers.
  absl::StatusOr<RegisteredMemory> RegisterRdmaMemory(
      const RdmaProtectionDomain& domain, std::span<std::byte> memory,
      RdmaAccess access) {
    return resources_.Register(domain, memory, access);
  }
  // Channel readiness uses the owner's single ring. The caller retains fd/tag
  // through terminal completion, including cancellation, as for kernel I/O.
  absl::Status SubmitRdmaEventPoll(int fd, unsigned events, IoCompletion* tag) {
    return Kernel().SubmitPoll(fd, events, tag);
  }
  absl::Status CancelRdmaEventPoll(IoCompletion* tag) {
    return Kernel().SubmitCancel(tag);
  }
  // Absolute CLOCK_MONOTONIC deadline; caller retains deadline/tag until its
  // terminal completion, including deferred submission and cancellation.
  absl::Status SubmitRdmaDeadline(const __kernel_timespec& deadline,
                                  IoCompletion* tag) {
    return Kernel().SubmitDeadline(deadline, tag);
  }
  // Stop admission before the ring shuts down. Tags/resources remain retained
  // until terminal completions or AfterRingShutdown confirms kernel quiescence.
  void Shutdown() { rdma::detail::BeginShutdown(engine_); }
  void AfterRingShutdown() {
    rdma::detail::AfterRingShutdown(engine_);
    engine_.reset();
    resources_.Shutdown();
  }
  absl::StatusOr<std::shared_ptr<rdma::detail::Engine>> GetRdmaEngine() {
    if (!engine_) {
      auto created = rdma::detail::CreateEngine(static_cast<Owner&>(*this));
      if (!created.ok()) return created.status();
      engine_ = *created;
    }
    return engine_;
  }

 private:
  friend Owner;
  RdmaBackend() = default;
  ~RdmaBackend() = default;
  RdmaBackend(const RdmaBackend&) = delete;
  RdmaBackend& operator=(const RdmaBackend&) = delete;
  IoUringBackend& Kernel() noexcept {
    return static_cast<Owner&>(*this).KernelBackend();
  }
  detail::RdmaResources resources_;
  std::shared_ptr<rdma::detail::Engine> engine_;
};
}  // namespace bycorf
#endif  // BYCORF_ENABLE_RDMA
#endif  // BYCORF_IO_RDMA_BACKEND_H_
