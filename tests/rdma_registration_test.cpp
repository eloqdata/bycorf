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
#include <rdma/rdma_cma.h>

#include <array>
#include <cstdlib>
#include <memory>

#include "bycorf/runtime/worker.h"

namespace {
using bycorf::RdmaAccess;
using bycorf::RdmaLocalAccess;

TEST(RdmaRegistration, SoftRoceRegistrationAndQpSharing) {
  const char* name = std::getenv("BYCORF_RDMA_TEST_DEVICE");
  if (!name || !*name) GTEST_SKIP() << "Set BYCORF_RDMA_TEST_DEVICE to opt in";
  // Backing memory is declared first: it survives every registration reference.
  std::array<std::byte, 4096> backing{};
  bycorf::Worker worker, other;
  auto domain = worker.OpenRdmaDevice(name);
  ASSERT_TRUE(domain.ok()) << domain.status();
  auto again = worker.OpenRdmaDevice(name);
  ASSERT_TRUE(again.ok());
  EXPECT_EQ(domain->native_pd(), again->native_pd());
  auto foreign = other.OpenRdmaDevice(name);
  ASSERT_TRUE(foreign.ok()) << foreign.status();
  EXPECT_NE(domain->native_pd(), foreign->native_pd());
  EXPECT_FALSE(
      worker.RegisterRdmaMemory(*foreign, backing, RdmaAccess::kNone).ok());
  auto mr = worker.RegisterRdmaMemory(*domain, backing,
                                      RdmaAccess::kLocalWrite |
                                          RdmaAccess::kRemoteRead |
                                          RdmaAccess::kRemoteWrite);
  ASSERT_TRUE(mr.ok()) << mr.status();
  auto slice = mr->Slice(64, 128);
  ASSERT_TRUE(slice.ok());
  EXPECT_FALSE(mr->Slice(4090, 7).ok());
  auto remote = slice->RemoteRegion(RdmaAccess::kRemoteWrite);
  ASSERT_TRUE(remote.ok());
  EXPECT_EQ(remote->address, reinterpret_cast<uintptr_t>(backing.data() + 64));
  EXPECT_EQ(remote->length, 128);
  auto* pd = domain->native_pd();
  std::unique_ptr<ibv_cq, decltype(&ibv_destroy_cq)> cq(
      ibv_create_cq(pd->context, 16, nullptr, nullptr, 0), ibv_destroy_cq);
  ASSERT_NE(cq, nullptr);
  ibv_qp_init_attr attr{};
  attr.send_cq = attr.recv_cq = cq.get();
  attr.qp_type = IBV_QPT_RC;
  attr.cap.max_send_wr = attr.cap.max_recv_wr = 4;
  attr.cap.max_send_sge = attr.cap.max_recv_sge = 1;
  std::unique_ptr<ibv_qp, decltype(&ibv_destroy_qp)> first(
      ibv_create_qp(pd, &attr), ibv_destroy_qp);
  std::unique_ptr<ibv_qp, decltype(&ibv_destroy_qp)> second(
      ibv_create_qp(pd, &attr), ibv_destroy_qp);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  auto lease = slice->Acquire(*first, RdmaLocalAccess::kWrite);
  ASSERT_TRUE(lease.ok());
  EXPECT_TRUE(slice->Acquire(*second, RdmaLocalAccess::kWrite).ok());
  auto read_only =
      worker.RegisterRdmaMemory(*domain, backing, RdmaAccess::kNone);
  ASSERT_TRUE(read_only.ok());
  auto readonly_slice = read_only->Slice(0, 64);
  ASSERT_TRUE(readonly_slice.ok());
  EXPECT_TRUE(readonly_slice->Acquire(*first, RdmaLocalAccess::kRead).ok());
  EXPECT_TRUE(absl::IsPermissionDenied(
      readonly_slice->Acquire(*first, RdmaLocalAccess::kWrite).status()));
  EXPECT_TRUE(absl::IsPermissionDenied(
      readonly_slice->RemoteRegion(RdmaAccess::kRemoteRead).status()));
  EXPECT_FALSE(
      worker.RegisterRdmaMemory(*domain, backing, RdmaAccess::kRemoteWrite)
          .ok());
  EXPECT_FALSE(
      worker.RegisterRdmaMemory(*domain, backing, RdmaAccess::kRemoteAtomic)
          .ok());
  // Another real PD on the same actual context must fail the SGE check.
  auto context = std::shared_ptr<ibv_context>(
      std::make_shared<bycorf::RdmaProtectionDomain>(*domain), pd->context);
  auto other_pd = other.AcquireRdmaDomain(context);
  ASSERT_TRUE(other_pd.ok());
  std::unique_ptr<ibv_qp, decltype(&ibv_destroy_qp)> wrong(
      ibv_create_qp(other_pd->native_pd(), &attr), ibv_destroy_qp);
  ASSERT_NE(wrong, nullptr);
  EXPECT_FALSE(slice->Acquire(*wrong, RdmaLocalAccess::kRead).ok());
  // Drop public registration handles: the operation lease still owns the MR.
  mr = bycorf::RegisteredMemory{};
  slice = bycorf::RegisteredMemorySlice{};
  EXPECT_EQ(lease->sge().length, 128);
  lease = bycorf::RdmaMemoryLease{};
  // Exercise deregistration/re-registration repeatedly on the actual provider.
  for (int i = 0; i < 32; ++i) {
    auto transient =
        worker.RegisterRdmaMemory(*domain, backing, RdmaAccess::kLocalWrite);
    ASSERT_TRUE(transient.ok()) << transient.status();
  }
}
}  // namespace
