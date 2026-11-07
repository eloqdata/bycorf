// Copyright (C) 2026 EloqData Inc.
// SPDX-License-Identifier: Apache-2.0
#include "dpdk_socket_table.h"

#include <gtest/gtest.h>

#include <array>
#include <utility>
#include <vector>

namespace bycorf::detail {
namespace {
// The table never dereferences a BSD socket; distinct addresses model distinct
// transports without EAL or a physical NIC.
struct socket* Socket(unsigned index) {
  static std::array<std::byte, 256> transports;
  return reinterpret_cast<struct socket*>(&transports.at(index));
}

TEST(DpdkSocketTableTest, ChurnDoesNotConsumeHandleNamespace) {
  DpdkSocketTable table;
  table.Configure(0x40000000, 2);
  const int listener = table.Insert(Socket(0));
  for (unsigned i = 0; i < 10000; ++i) {
    const int connection = table.Insert(Socket(1));
    ASSERT_EQ(connection, listener + 1);
    EXPECT_EQ(table.Insert(Socket(2)), -1);
    EXPECT_EQ(table.Close(connection), Socket(1));
  }
  EXPECT_EQ(table.Close(listener), Socket(0));
}

TEST(DpdkSocketTableTest, CloseWaitsForEveryOperationAndCompletion) {
  DpdkSocketTable table;
  table.Configure(0x40000000, 1);
  const int old = table.Insert(Socket(0));
  auto send = table.Acquire(old);
  auto connect = table.Acquire(old);
  auto completion = send;
  EXPECT_EQ(table.Close(old), Socket(0));
  EXPECT_EQ(send.get(), nullptr);
  EXPECT_FALSE(table.Acquire(old));
  EXPECT_EQ(table.Insert(Socket(1)), -1);
  send.Reset();
  connect.Reset();
  EXPECT_EQ(table.Insert(Socket(1)), -1);
  completion.Reset();
  EXPECT_EQ(table.Insert(Socket(1)), old);
  EXPECT_EQ(table.Close(old), Socket(1));
}

TEST(DpdkSocketTableTest, CallbackCannotReuseItsOwnClosedHandle) {
  DpdkSocketTable table;
  table.Configure(0x40000000, 2);
  const int old = table.Insert(Socket(0));
  int replacement;
  {
    auto completion = table.Acquire(old);
    ASSERT_EQ(table.Close(old), Socket(0));
    replacement = table.Insert(Socket(1));
    EXPECT_NE(replacement, old);
    EXPECT_EQ(completion.get(), nullptr);
  }
  EXPECT_EQ(table.Insert(Socket(2)), old);
  EXPECT_EQ(table.Close(old), Socket(2));
  EXPECT_EQ(table.Close(replacement), Socket(1));
}

TEST(DpdkSocketTableTest, PinsSurviveTableAndQueueGrowth) {
  DpdkSocketTable table;
  table.Configure(0x40000000, 128);
  std::vector<DpdkSocketTable::Pin> operations;
  std::vector<int> handles;
  for (unsigned i = 0; i < 128; ++i) {
    handles.push_back(table.Insert(Socket(i)));
    operations.push_back(table.Acquire(handles.back()));
  }
  for (unsigned i = 0; i < handles.size(); ++i) {
    EXPECT_EQ(operations[i].get(), Socket(i));
    EXPECT_EQ(table.Close(handles[i]), Socket(i));
    EXPECT_EQ(operations[i].get(), nullptr);
  }
  EXPECT_EQ(table.Insert(Socket(128)), -1);
  // Pending vectors compact on cancellation/completion; moving a pin must
  // release its old slot exactly once and retain its replacement's identity.
  operations.erase(operations.begin());
  EXPECT_EQ(table.Insert(Socket(128)), handles.front());
  EXPECT_EQ(table.Insert(Socket(129)), -1);
  EXPECT_EQ(table.Close(handles.front()), Socket(128));
  operations.clear();
}

TEST(DpdkSocketTableTest, WorkerRangesAndRepeatedCloseStayIsolated) {
  DpdkSocketTable first, second;
  first.Configure(0x40000000, 2);
  second.Configure(0x40800000, 2);
  const int a = first.Insert(Socket(0)), b = second.Insert(Socket(1));
  EXPECT_EQ(first.Find(b), nullptr);
  EXPECT_EQ(second.Close(a), nullptr);
  EXPECT_EQ(first.Find(-1), nullptr);
  EXPECT_EQ(first.Close(a), Socket(0));
  EXPECT_EQ(first.Close(a), nullptr);
  EXPECT_EQ(first.Insert(Socket(2)), a);
  EXPECT_NE(first.Insert(Socket(3)), a);
  EXPECT_EQ(first.Insert(Socket(4)), -1);
  first.CloseAll([](struct socket*) {});
  second.CloseAll([](struct socket*) {});
}

TEST(DpdkSocketTableTest, ShutdownClosesTransportsBeforeDrainingPins) {
  DpdkSocketTable table;
  table.Configure(0x40000000, 2);
  auto a = table.Acquire(table.Insert(Socket(0)));
  auto b = table.Acquire(table.Insert(Socket(1)));
  unsigned closed = 0;
  table.CloseAll([&](struct socket*) { ++closed; });
  EXPECT_EQ(closed, 2U);
  EXPECT_EQ(a.get(), nullptr);
  EXPECT_EQ(b.get(), nullptr);
  EXPECT_EQ(table.Insert(Socket(2)), -1);
  a.Reset();
  b.Reset();
  table.Configure(0x40800000, 1);
  EXPECT_EQ(table.Insert(Socket(2)), 0x40800000);
  EXPECT_EQ(table.Close(0x40800000), Socket(2));
}
}  // namespace
}  // namespace bycorf::detail
