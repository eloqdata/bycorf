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

#include "bycorf/io/rdma_memory.h"

#include <algorithm>
#include <cerrno>
#include <exception>
#include <limits>

#include "absl/strings/str_cat.h"
#include "spdlog/spdlog.h"

namespace bycorf {
namespace {
constexpr unsigned kLocalWrite = static_cast<unsigned>(RdmaAccess::kLocalWrite);
constexpr unsigned kRemoteWrite =
    static_cast<unsigned>(RdmaAccess::kRemoteWrite);
constexpr unsigned kRemoteRead = static_cast<unsigned>(RdmaAccess::kRemoteRead);
constexpr unsigned kRemoteAtomic =
    static_cast<unsigned>(RdmaAccess::kRemoteAtomic);
constexpr unsigned kRemote = kRemoteWrite | kRemoteRead | kRemoteAtomic;

absl::Status VerbsError(const char* operation) {
  const int error = errno;
  return absl::ErrnoToStatus(error, absl::StrCat(operation, " failed"));
}

// A failed teardown may leave resources referenced by hardware. Continuing
// could release their PD/context too early. Destructors cannot report errors,
// so fail closed instead of hiding a lifetime violation or provider failure.
void CheckRelease(int result, const char* operation) noexcept {
  if (result != 0) {
    spdlog::critical("{} failed during RDMA resource teardown: {}", operation,
                     result);
    std::terminate();
  }
}

absl::Status ValidateRange(std::size_t total, std::size_t offset,
                           std::size_t length) {
  if (length == 0 || offset > total || length > total - offset) {
    return absl::InvalidArgumentError("empty or out-of-bounds RDMA range");
  }
  return absl::OkStatus();
}
}  // namespace

namespace detail {
struct RdmaDomainState {
  std::shared_ptr<ibv_context> context;
  ibv_pd* pd = nullptr;
  ~RdmaDomainState() {
    if (pd) CheckRelease(ibv_dealloc_pd(pd), "ibv_dealloc_pd");
  }
};

struct RdmaRegistration {
  std::shared_ptr<RdmaDomainState> domain;
  ibv_mr* mr = nullptr;
  RdmaAccess access = RdmaAccess::kNone;
  ~RdmaRegistration() {
    if (mr) CheckRelease(ibv_dereg_mr(mr), "ibv_dereg_mr");
  }
};

absl::StatusOr<RdmaProtectionDomain> RdmaResources::Domain(
    const std::shared_ptr<ibv_context>& context) {
  if (!context || context.use_count() == 0) {
    return absl::InvalidArgumentError("RDMA context must have an owner");
  }
  std::erase_if(domains_, [](const auto& weak) { return weak.expired(); });
  for (const auto& weak : domains_) {
    if (auto state = weak.lock();
        state && state->context.get() == context.get()) {
      RdmaProtectionDomain result;
      result.state_ = std::move(state);
      return result;
    }
  }
  // Allocate the C++ owner before acquiring verbs resources, including when
  // cache growth later throws. Each failed attempt leaves no partial PD behind.
  auto state = std::make_shared<RdmaDomainState>();
  state->context = context;
  state->pd = ibv_alloc_pd(context.get());
  if (!state->pd) return VerbsError("ibv_alloc_pd");
  domains_.push_back(state);
  RdmaProtectionDomain result;
  result.state_ = std::move(state);
  return result;
}

absl::StatusOr<RdmaProtectionDomain> RdmaResources::OpenDevice(
    std::string_view name) {
  if (name.empty())
    return absl::InvalidArgumentError("RDMA device name is empty");
  for (const auto& weak : domains_) {
    if (auto state = weak.lock();
        state && name == ibv_get_device_name(state->context->device)) {
      RdmaProtectionDomain result;
      result.state_ = std::move(state);
      return result;
    }
  }
  int count = 0;
  std::unique_ptr<ibv_device*, decltype(&ibv_free_device_list)> devices(
      ibv_get_device_list(&count), ibv_free_device_list);
  if (!devices) return VerbsError("ibv_get_device_list");
  for (int i = 0; i < count; ++i) {
    if (name != ibv_get_device_name(devices.get()[i])) continue;
    auto* opened = ibv_open_device(devices.get()[i]);
    if (!opened) return VerbsError("ibv_open_device");
    // shared_ptr invokes this deleter even if allocating its control block
    // throws, so a successful open never escapes without an owner.
    std::shared_ptr<ibv_context> context(opened, [](ibv_context* value) {
      CheckRelease(ibv_close_device(value), "ibv_close_device");
    });
    return Domain(context);
  }
  return absl::NotFoundError(absl::StrCat("RDMA device not found: ", name));
}

absl::StatusOr<RegisteredMemory> RdmaResources::Register(
    const RdmaProtectionDomain& domain, std::span<std::byte> memory,
    RdmaAccess access) {
  if (!domain ||
      !std::any_of(domains_.begin(), domains_.end(), [&](const auto& weak) {
        return weak.lock() == domain.state_;
      })) {
    return absl::InvalidArgumentError("RDMA PD does not belong to this worker");
  }
  const auto address = reinterpret_cast<std::uintptr_t>(memory.data());
  if (!memory.data() || memory.empty() ||
      memory.size() > std::numeric_limits<std::uintptr_t>::max() - address) {
    return absl::InvalidArgumentError("empty or overflowing RDMA memory range");
  }
  const auto flags = static_cast<unsigned>(access);
  if ((flags & ~(kLocalWrite | kRemote)) != 0 ||
      ((flags & (kRemoteWrite | kRemoteAtomic)) && !(flags & kLocalWrite))) {
    return absl::InvalidArgumentError("invalid RDMA registration permissions");
  }
  auto registration = std::make_shared<RdmaRegistration>();
  registration->domain = domain.state_;
  registration->access = access;
  registration->mr = ibv_reg_mr(domain.native_pd(), memory.data(),
                                memory.size(), static_cast<int>(flags));
  if (!registration->mr) return VerbsError("ibv_reg_mr");
  RegisteredMemory result;
  result.registration_ = std::move(registration);
  return result;
}
}  // namespace detail

ibv_pd* RdmaProtectionDomain::native_pd() const noexcept {
  return state_ ? state_->pd : nullptr;
}

std::size_t RegisteredMemory::size() const noexcept {
  return registration_ ? registration_->mr->length : 0;
}
RdmaAccess RegisteredMemory::access() const noexcept {
  return registration_ ? registration_->access : RdmaAccess::kNone;
}
absl::StatusOr<RegisteredMemorySlice> RegisteredMemory::Slice(
    std::size_t offset, std::size_t length) const {
  auto status = ValidateRange(size(), offset, length);
  if (!status.ok()) return status;
  RegisteredMemorySlice result;
  result.registration_ = registration_;
  result.offset_ = offset;
  result.length_ = length;
  return result;
}
absl::StatusOr<RegisteredMemorySlice> RegisteredMemorySlice::Slice(
    std::size_t offset, std::size_t length) const {
  auto status = ValidateRange(size(), offset, length);
  if (!status.ok()) return status;
  RegisteredMemorySlice result = *this;
  result.offset_ += offset;
  result.length_ = length;
  return result;
}
absl::StatusOr<RdmaMemoryLease> RegisteredMemorySlice::Acquire(
    const ibv_qp& qp, RdmaLocalAccess access) const {
  if (!registration_) return absl::FailedPreconditionError("empty RDMA slice");
  const auto& domain = *registration_->domain;
  if (qp.pd != domain.pd || qp.context != domain.context.get()) {
    return absl::InvalidArgumentError(
        "RDMA QP and MR must share context and PD");
  }
  if (access != RdmaLocalAccess::kRead && access != RdmaLocalAccess::kWrite) {
    return absl::InvalidArgumentError("invalid RDMA local access");
  }
  if (access == RdmaLocalAccess::kWrite &&
      !(static_cast<unsigned>(registration_->access) & kLocalWrite)) {
    return absl::PermissionDeniedError("MR does not allow local writes");
  }
  if (length_ > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError("RDMA slice exceeds one SGE's length");
  }
  RdmaMemoryLease result;
  result.registration_ = registration_;
  result.sge_.addr =
      reinterpret_cast<std::uintptr_t>(registration_->mr->addr) + offset_;
  result.sge_.length = static_cast<std::uint32_t>(length_);
  result.sge_.lkey = registration_->mr->lkey;
  return result;
}
absl::StatusOr<RemoteMemoryRegion> RegisteredMemorySlice::RemoteRegion(
    RdmaAccess access) const {
  if (!registration_) return absl::FailedPreconditionError("empty RDMA slice");
  const auto requested = static_cast<unsigned>(access);
  if (!requested || (requested & ~kRemote)) {
    return absl::InvalidArgumentError("expected explicit remote permissions");
  }
  if ((requested & static_cast<unsigned>(registration_->access)) != requested) {
    return absl::PermissionDeniedError("MR lacks requested remote permissions");
  }
  return RemoteMemoryRegion{
      reinterpret_cast<std::uintptr_t>(registration_->mr->addr) + offset_,
      length_, registration_->mr->rkey};
}
absl::StatusOr<RemoteMemoryRegion> RemoteMemoryRegion::Slice(
    std::uint64_t offset, std::uint64_t size) const {
  if (!size || offset > length || size > length - offset ||
      length > std::numeric_limits<std::uint64_t>::max() - address) {
    return absl::InvalidArgumentError(
        "empty, overflowing or out-of-bounds remote range");
  }
  return RemoteMemoryRegion{address + offset, size, rkey};
}
}  // namespace bycorf
