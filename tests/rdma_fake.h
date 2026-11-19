// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <cstddef>
#include <cstdint>

namespace rdma_fake {
void Reset();
void CheckEmpty();
// Select one of three independent fake devices for subsequently created CM IDs.
void SelectDevice(unsigned index);
std::size_t PendingCompletions(ibv_cq* cq);
void SuppressConnect(bool suppress);
void SuppressEstablish(bool suppress);
void Cm(rdma_cm_id* id, rdma_cm_event_type event, int status = 0);
void RoundQp(unsigned extra);
void FailNextQp();
void FailNextNotify();
void FailNextPost(int error, bool receive = false);
void HoldTransfers(bool hold);
void DropFlush(bool drop);
int LiveMrs();
int Posts(bool receive);
std::uint32_t WireImmediate();
// Suppress the next notification's fd wake, retaining the CQ event. A test may
// write to the returned fd from another thread without accessing fake state.
int DeferNotification(ibv_cq* cq);
void Completion(ibv_cq* cq, std::uint32_t qp,
                ibv_wc_status status = IBV_WC_SUCCESS);
void Async(ibv_context* context, ibv_event_type type, ibv_qp* qp = nullptr);
// Inject a CQE exactly when notification is requested, before the CQ recheck.
void CompleteOnArm(ibv_cq* cq, std::uint32_t qp);
int CqPolls(ibv_cq* cq);
int Notifications(ibv_cq* cq);
int AckedCqEvents();
int AckedAsyncEvents();
int LiveQps();
int LiveIds();
}  // namespace rdma_fake
