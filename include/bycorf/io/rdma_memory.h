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

#ifndef BYCORF_IO_RDMA_MEMORY_H_
#define BYCORF_IO_RDMA_MEMORY_H_

#if BYCORF_ENABLE_RDMA
#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"

namespace bycorf {

// Explicit registration permissions. Local reads are always allowed by verbs.
// Remote write/atomic require kLocalWrite; unsupported/unknown bits are
// rejected.
enum class RdmaAccess : unsigned {
  kNone = 0,
  kLocalWrite = IBV_ACCESS_LOCAL_WRITE,
  kRemoteWrite = IBV_ACCESS_REMOTE_WRITE,
  kRemoteRead = IBV_ACCESS_REMOTE_READ,
  kRemoteAtomic = IBV_ACCESS_REMOTE_ATOMIC,
};
constexpr RdmaAccess operator|(RdmaAccess a, RdmaAccess b) noexcept {
  return static_cast<RdmaAccess>(static_cast<unsigned>(a) |
                                 static_cast<unsigned>(b));
}

// A peer descriptor, not an owning handle or a wire serialization format.
// The application exchanges it and revokes remote access before deregistration.
// A sliced descriptor narrows our range checks, not the hardware rkey's
// authority.
struct RemoteMemoryRegion {
  std::uint64_t address = 0;
  std::uint64_t length = 0;
  std::uint32_t rkey = 0;

  // Rejects empty, overflowing, or out-of-bounds subranges.
  absl::StatusOr<RemoteMemoryRegion> Slice(std::uint64_t offset,
                                           std::uint64_t size) const;
};

namespace detail {
struct RdmaDomainState;
struct RdmaRegistration;
class RdmaResources;
}  // namespace detail

// Retains a PD and its actual verbs context. QP owners must retain this handle
// until after destroying their QPs. Multiple QPs in this PD may use every MR
// registered in it; an MR is not owned by a QP or an immediate-data operation.
class RdmaProtectionDomain {
 public:
  RdmaProtectionDomain() = default;
  // Borrowed for QP construction; never deallocate it outside this owner.
  ibv_pd* native_pd() const noexcept;
  explicit operator bool() const noexcept { return state_ != nullptr; }

 private:
  friend class detail::RdmaResources;
  std::shared_ptr<detail::RdmaDomainState> state_;
};

enum class RdmaLocalAccess { kRead, kWrite };

// An operation retains this lease until terminal completion (including error
// and cancellation). It owns registration, PD and context, NEVER backing
// memory. The QP's separate owner must also survive completion. Copying the SGE
// alone does not retain any resources.
class RdmaMemoryLease {
 public:
  const ibv_sge& sge() const noexcept { return sge_; }

 private:
  friend class RegisteredMemorySlice;
  std::shared_ptr<detail::RdmaRegistration> registration_;
  ibv_sge sge_{};
};

// Copyable registration-retaining view; slices do not register another MR.
class RegisteredMemorySlice {
 public:
  RegisteredMemorySlice() = default;
  std::size_t size() const noexcept { return registration_ ? length_ : 0; }
  absl::StatusOr<RegisteredMemorySlice> Slice(std::size_t offset,
                                              std::size_t length) const;
  // Checks actual QP context/PD, local permission and the 32-bit SGE length.
  absl::StatusOr<RdmaMemoryLease> Acquire(const ibv_qp& qp,
                                          RdmaLocalAccess access) const;
  // Requires a nonempty subset of granted remote permissions. The returned
  // descriptor does not retain this registration or enforce peer cooperation.
  absl::StatusOr<RemoteMemoryRegion> RemoteRegion(RdmaAccess access) const;

 private:
  friend class RegisteredMemory;
  std::shared_ptr<detail::RdmaRegistration> registration_;
  std::size_t offset_ = 0;
  std::size_t length_ = 0;
};

// Owns only the verbs registration. Copies, slices and operation leases retain
// it; the last reference deregisters the MR before releasing PD/context.
// Caller keeps the original allocation alive through all local completions AND
// remote quiescence, and releases all registration references before freeing
// it. Storage RegisterBuffers is an independent registration/lifetime contract.
class RegisteredMemory {
 public:
  RegisteredMemory() = default;
  std::size_t size() const noexcept;
  RdmaAccess access() const noexcept;
  absl::StatusOr<RegisteredMemorySlice> Slice(std::size_t offset,
                                              std::size_t length) const;

 private:
  friend class detail::RdmaResources;
  std::shared_ptr<detail::RdmaRegistration> registration_;
};

namespace detail {
// Worker-local weak cache. No allocation/device access until explicit use;
// handles, not the worker cache, determine resource lifetimes. Calls are
// confined to the owner worker. No global registries, blocking locks or memory
// pools.
class RdmaResources {
 public:
  absl::StatusOr<RdmaProtectionDomain> OpenDevice(std::string_view name);
  absl::StatusOr<RdmaProtectionDomain> Domain(
      const std::shared_ptr<ibv_context>& context);
  absl::StatusOr<RegisteredMemory> Register(const RdmaProtectionDomain& domain,
                                            std::span<std::byte> memory,
                                            RdmaAccess access);
  void Shutdown() noexcept { domains_.clear(); }

 private:
  std::vector<std::weak_ptr<RdmaDomainState>> domains_;
};
}  // namespace detail
}  // namespace bycorf
#endif  // BYCORF_ENABLE_RDMA
#endif  // BYCORF_IO_RDMA_MEMORY_H_
