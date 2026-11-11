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

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <limits>
#include <string>
#include <vector>

#include "bycorf/runtime/worker.h"

namespace {
struct VerbsState {
  int lists = 0, opens = 0, pds = 0, mrs = 0;
  int device_calls = 0;
  int last_access = -1;
  bool fail_list = false, fail_open = false, fail_pd = false, fail_mr = false;
  std::vector<std::string> releases;
} verbs;
ibv_device device{};

class RdmaMemory : public testing::Test {
 protected:
  void SetUp() override { verbs = {}; }
  void TearDown() override {
    EXPECT_EQ(verbs.lists, 0);
    EXPECT_EQ(verbs.opens, 0);
    EXPECT_EQ(verbs.pds, 0);
    EXPECT_EQ(verbs.mrs, 0);
  }
};
}  // namespace

extern "C" {
ibv_device** __wrap_ibv_get_device_list(int* count) {
  ++verbs.device_calls;
  if (verbs.fail_list) {
    errno = ENODEV;
    return nullptr;
  }
  ++verbs.lists;
  *count = 1;
  return new ibv_device*[2]{&device, nullptr};
}
void __wrap_ibv_free_device_list(ibv_device** devices) {
  --verbs.lists;
  delete[] devices;
}
const char* __wrap_ibv_get_device_name(ibv_device*) { return "test0"; }
ibv_context* __wrap_ibv_open_device(ibv_device* dev) {
  ++verbs.device_calls;
  if (verbs.fail_open) {
    errno = EACCES;
    return nullptr;
  }
  ++verbs.opens;
  auto* context = new ibv_context{};
  context->device = dev;
  return context;
}
int __wrap_ibv_close_device(ibv_context* context) {
  --verbs.opens;
  verbs.releases.push_back("context");
  delete context;
  return 0;
}
ibv_pd* __wrap_ibv_alloc_pd(ibv_context* context) {
  ++verbs.device_calls;
  if (verbs.fail_pd) {
    errno = ENOMEM;
    return nullptr;
  }
  ++verbs.pds;
  auto* pd = new ibv_pd{};
  pd->context = context;
  return pd;
}
int __wrap_ibv_dealloc_pd(ibv_pd* pd) {
  --verbs.pds;
  verbs.releases.push_back("pd");
  delete pd;
  return 0;
}
ibv_mr* __wrap_ibv_reg_mr(ibv_pd* pd, void* addr, size_t length, int access) {
  if (verbs.fail_mr) {
    errno = ENOMEM;
    return nullptr;
  }
  ++verbs.mrs;
  verbs.last_access = access;
  auto* mr = new ibv_mr{};
  mr->context = pd->context;
  mr->pd = pd;
  mr->addr = addr;
  mr->length = length;
  mr->lkey = 17;
  mr->rkey = 29;
  return mr;
}
// verbs headers dispatch runtime access flags through iova2; optimized builds
// may use the older entry point. Intercept both without changing production.
ibv_mr* __wrap_ibv_reg_mr_iova2(ibv_pd* pd, void* addr, size_t length,
                                uint64_t iova, unsigned access) {
  EXPECT_EQ(iova, reinterpret_cast<uintptr_t>(addr));
  return __wrap_ibv_reg_mr(pd, addr, length, static_cast<int>(access));
}
int __wrap_ibv_dereg_mr(ibv_mr* mr) {
  --verbs.mrs;
  verbs.releases.push_back("mr");
  delete mr;
  return 0;
}
}  // extern "C"

namespace {
using bycorf::RdmaAccess;
using bycorf::RdmaLocalAccess;

TEST_F(RdmaMemory, UnusedWorkerNeverAccessesDevices) {
  verbs.fail_list = verbs.fail_open = verbs.fail_pd = true;
  {
    bycorf::Worker worker;
    worker.Shutdown();
  }
  EXPECT_TRUE(verbs.releases.empty());
  EXPECT_EQ(verbs.device_calls, 0);
}

TEST_F(RdmaMemory, FailedInitializationReleasesPartialResourcesAndCanRetry) {
  bycorf::Worker worker;
  verbs.fail_list = true;
  EXPECT_FALSE(worker.OpenRdmaDevice("test0").ok());
  verbs.fail_list = false;
  verbs.fail_open = true;
  EXPECT_FALSE(worker.OpenRdmaDevice("test0").ok());
  EXPECT_EQ(verbs.lists, 0);
  verbs.fail_open = false;
  verbs.fail_pd = true;
  EXPECT_FALSE(worker.OpenRdmaDevice("test0").ok());
  EXPECT_EQ(verbs.opens, 0);
  EXPECT_EQ(verbs.pds, 0);
  verbs.fail_pd = false;
  auto domain = worker.OpenRdmaDevice("test0");
  ASSERT_TRUE(domain.ok()) << domain.status();
  std::array<std::byte, 32> memory;
  verbs.fail_mr = true;
  EXPECT_FALSE(
      worker.RegisterRdmaMemory(*domain, memory, RdmaAccess::kNone).ok());
  EXPECT_EQ(verbs.mrs, 0);
  EXPECT_EQ(verbs.pds, 1);
  verbs.fail_mr = false;
  EXPECT_TRUE(
      worker.RegisterRdmaMemory(*domain, memory, RdmaAccess::kNone).ok());
  EXPECT_FALSE(worker.OpenRdmaDevice("missing").ok());
  EXPECT_FALSE(worker.OpenRdmaDevice("").ok());
  EXPECT_FALSE(worker.AcquireRdmaDomain({}).ok());
}

TEST_F(RdmaMemory, ContextIdentityAndWorkerOwnership) {
  bycorf::Worker first, second;
  auto a = first.OpenRdmaDevice("test0");
  auto b = first.OpenRdmaDevice("test0");
  ASSERT_TRUE(a.ok());
  ASSERT_TRUE(b.ok());
  EXPECT_EQ(a->native_pd(), b->native_pd());
  // Same device, distinct actual context: must not reuse the first PD.
  auto context = std::shared_ptr<ibv_context>(__wrap_ibv_open_device(&device),
                                              __wrap_ibv_close_device);
  auto c = first.AcquireRdmaDomain(context);
  auto d = second.AcquireRdmaDomain(context);
  ASSERT_TRUE(c.ok());
  ASSERT_TRUE(d.ok());
  EXPECT_NE(a->native_pd(), c->native_pd());
  EXPECT_NE(c->native_pd(), d->native_pd());
  EXPECT_EQ(c->native_pd()->context, d->native_pd()->context);
  std::array<std::byte, 32> memory;
  EXPECT_FALSE(first.RegisterRdmaMemory(*d, memory, RdmaAccess::kNone).ok());
  EXPECT_FALSE(first.RegisterRdmaMemory({}, memory, RdmaAccess::kNone).ok());
}

TEST_F(RdmaMemory, LeaseKeepsRegistrationAndContextAfterWorkerDestruction) {
  std::array<std::byte, 64> backing;
  bycorf::RdmaMemoryLease retained;
  {
    bycorf::Worker worker;
    auto domain = worker.OpenRdmaDevice("test0");
    ASSERT_TRUE(domain.ok());
    auto mr = worker.RegisterRdmaMemory(
        *domain, backing, RdmaAccess::kLocalWrite | RdmaAccess::kRemoteWrite);
    ASSERT_TRUE(mr.ok());
    auto slice = mr->Slice(8, 24);
    ASSERT_TRUE(slice.ok());
    auto sub = slice->Slice(4, 8);
    ASSERT_TRUE(sub.ok());
    ibv_qp qp{};
    qp.pd = domain->native_pd();
    qp.context = qp.pd->context;
    auto lease = sub->Acquire(qp, RdmaLocalAccess::kWrite);
    ASSERT_TRUE(lease.ok());
    retained = *lease;
    EXPECT_EQ(retained.sge().addr,
              reinterpret_cast<uintptr_t>(backing.data() + 12));
    EXPECT_EQ(retained.sge().length, 8);
    EXPECT_EQ(retained.sge().lkey, 17);
    worker.Shutdown();
    EXPECT_EQ(verbs.mrs, 1);
  }
  EXPECT_EQ(verbs.mrs, 1);
  EXPECT_EQ(verbs.pds, 1);
  EXPECT_EQ(verbs.opens, 1);
  retained = {};
  EXPECT_EQ(verbs.releases, (std::vector<std::string>{"mr", "pd", "context"}));
}

TEST_F(RdmaMemory, PermissionsRangesAndProtectionDomainChecks) {
  std::array<std::byte, 64> backing;
  bycorf::Worker worker;
  auto domain = worker.OpenRdmaDevice("test0");
  ASSERT_TRUE(domain.ok());
  EXPECT_FALSE(worker.RegisterRdmaMemory(*domain, {}, RdmaAccess::kNone).ok());
  for (auto invalid : {RdmaAccess::kRemoteWrite, RdmaAccess::kRemoteAtomic,
                       static_cast<RdmaAccess>(1U << 30)}) {
    EXPECT_FALSE(worker.RegisterRdmaMemory(*domain, backing, invalid).ok());
  }
  auto mr =
      worker.RegisterRdmaMemory(*domain, backing, RdmaAccess::kRemoteRead);
  ASSERT_TRUE(mr.ok());
  EXPECT_EQ(verbs.last_access, IBV_ACCESS_REMOTE_READ);
  EXPECT_EQ(mr->access(), RdmaAccess::kRemoteRead);
  EXPECT_EQ(mr->size(), backing.size());
  EXPECT_FALSE(mr->Slice(0, 0).ok());
  EXPECT_FALSE(mr->Slice(64, 1).ok());
  EXPECT_FALSE(mr->Slice(1, std::numeric_limits<size_t>::max()).ok());
  auto slice = mr->Slice(8, 32);
  ASSERT_TRUE(slice.ok());
  EXPECT_FALSE(slice->Slice(31, 2).ok());
  ibv_qp qp{};
  qp.pd = domain->native_pd();
  qp.context = qp.pd->context;
  EXPECT_TRUE(slice->Acquire(qp, RdmaLocalAccess::kRead).ok());
  EXPECT_TRUE(absl::IsPermissionDenied(
      slice->Acquire(qp, RdmaLocalAccess::kWrite).status()));
  ibv_pd wrong{};
  wrong.context = qp.context;
  qp.pd = &wrong;
  EXPECT_FALSE(slice->Acquire(qp, RdmaLocalAccess::kRead).ok());
  qp.pd = domain->native_pd();
  qp.context = nullptr;
  EXPECT_FALSE(slice->Acquire(qp, RdmaLocalAccess::kRead).ok());
  auto remote = slice->RemoteRegion(RdmaAccess::kRemoteRead);
  ASSERT_TRUE(remote.ok());
  EXPECT_EQ(remote->address, reinterpret_cast<uintptr_t>(backing.data() + 8));
  EXPECT_EQ(remote->length, 32);
  EXPECT_EQ(remote->rkey, 29);
  EXPECT_FALSE(slice->RemoteRegion(RdmaAccess::kNone).ok());
  EXPECT_FALSE(slice->RemoteRegion(RdmaAccess::kLocalWrite).ok());
  EXPECT_TRUE(absl::IsPermissionDenied(
      slice->RemoteRegion(RdmaAccess::kRemoteWrite).status()));
  EXPECT_FALSE(bycorf::RegisteredMemory{}.Slice(0, 1).ok());
  EXPECT_FALSE(bycorf::RegisteredMemorySlice{}
                   .RemoteRegion(RdmaAccess::kRemoteRead)
                   .ok());
}

TEST_F(RdmaMemory, SliceAloneRetainsRegistrationAndMovedViewIsEmpty) {
  std::array<std::byte, 64> backing;
  bycorf::Worker worker;
  auto domain = worker.OpenRdmaDevice("test0");
  ASSERT_TRUE(domain.ok());
  auto mr = worker.RegisterRdmaMemory(
      *domain, backing, RdmaAccess::kLocalWrite | RdmaAccess::kRemoteAtomic);
  ASSERT_TRUE(mr.ok());
  EXPECT_EQ(verbs.last_access,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
  auto slice = mr->Slice(0, 64);
  ASSERT_TRUE(slice.ok());
  mr = bycorf::RegisteredMemory{};
  EXPECT_EQ(verbs.mrs, 1);
  auto moved = std::move(*slice);
  EXPECT_EQ(slice->size(), 0);
  EXPECT_FALSE(slice->Slice(0, 1).ok());
  EXPECT_TRUE(moved.RemoteRegion(RdmaAccess::kRemoteAtomic).ok());
  moved = {};
  EXPECT_EQ(verbs.mrs, 0);
}

TEST_F(RdmaMemory, RemoteRangesCannotOverflow) {
  bycorf::RemoteMemoryRegion region{100, 50, 7};
  auto sub = region.Slice(10, 40);
  ASSERT_TRUE(sub.ok());
  EXPECT_EQ(sub->address, 110);
  EXPECT_EQ(sub->length, 40);
  EXPECT_EQ(sub->rkey, 7);
  EXPECT_FALSE(region.Slice(51, 1).ok());
  EXPECT_FALSE(region.Slice(0, 0).ok());
  EXPECT_FALSE(region.Slice(1, std::numeric_limits<uint64_t>::max()).ok());
  region.address = std::numeric_limits<uint64_t>::max() - 1;
  EXPECT_FALSE(region.Slice(0, 1).ok());
}
}  // namespace
