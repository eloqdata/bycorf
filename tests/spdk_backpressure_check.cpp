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

#include <fcntl.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

#include "bycorf/io/spdk_storage.h"
#include "bycorf/runtime/worker.h"

namespace {
struct Completion : bycorf::IoCompletion {
  unsigned calls = 0;
  int result = -1;
  unsigned* completed_total = nullptr;
  void Complete(bycorf::Worker&, int value, unsigned) override {
    ++calls;
    result = value;
    if (completed_total != nullptr) ++*completed_total;
  }
};

template <typename Done>
void Drain(bycorf::Worker& worker, Done done) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (!done()) {
    // An empty poll is not a failure; NVMe may complete on a later pass.
    worker.RunOnce(false);
    if (std::chrono::steady_clock::now() > deadline) {
      std::cerr << "FAIL timed out draining accepted I/O\n";
      // Never reclaim DMA buffers while the device may still use them.
      std::_Exit(1);
    }
  }
}
}  // namespace

// Read-only hardware regression: supply one or more explicitly allowed SPDK
// namespaces via argv and the ordinary BYCORF_EAL_ARGS device allowlist.
// Each burst exceeds the default qpair's 512 request descriptors. No polling
// occurs during submission, making queue exhaustion deterministic.
int main(int argc, char** argv) {
  if (argc < 2 || argc > 3) {
    std::cerr << "usage: spdk_backpressure_check spdk://BDF/1 [spdk://BDF/1]\n";
    return 2;
  }
  const auto configured = bycorf::ConfigureIoBackends({.spdk_storage = true});
  if (!configured.ok()) {
    std::cerr << configured << '\n';
    return 1;
  }
  for (int i = 1; i < argc; ++i) {
    const auto info = bycorf::ProbeSpdkStorage(argv[i]);
    if (!info.ok()) {
      std::cerr << info.status() << '\n';
      return 1;
    }
  }
  bycorf::ReleaseSpdkStorageMetadataQpairs();
  // Use the real one-backend-per-worker ownership and callback context.
  bycorf::CrossCore cross_core(1);
  const int wake_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (wake_fd < 0) return 1;
  cross_core.mailbox(0).wake_fd_ = wake_fd;
  bycorf::Worker worker;
  worker.BindCrossCore(0, &cross_core);
  bycorf::SetThisWorker(0, &cross_core, &worker);
  const unsigned files = argc - 1;
  if (!worker.Init({.spdk_max_completions_per_poll_ = 16}).ok() ||
      !worker.RegisterFixedFiles(files).ok())
    return 1;
  for (unsigned i = 0; i < files; ++i) {
    Completion opened;
    const auto status =
        worker.SubmitOpenDirect(argv[i + 1], O_RDWR, 0, {i}, &opened);
    if (!status.ok()) {
      std::cerr << status << '\n';
      return 1;
    }
    if (opened.calls != 0) return 1;
    Drain(worker, [&] { return opened.calls != 0; });
    if (opened.calls != 1 || opened.result != 0) return 1;
  }
  constexpr unsigned per_file = 1024;
  constexpr std::size_t bytes = 4096;
  const unsigned count = per_file * files;
  auto* buffers = static_cast<std::byte*>(
      bycorf::AllocateStorageBuffer(count * bytes, bytes));
  if (buffers == nullptr) return 1;
  bool passed = true;
  for (unsigned round = 0; round < 4; ++round) {
    std::vector<Completion> tags(count);
    std::vector<bool> accepted(count);
    unsigned rejected = 0;
    unsigned delivered = 0;
    for (unsigned i = 0; i < count; ++i) {
      tags[i].completed_total = &delivered;
      const auto status = worker.SubmitRead(
          {i % files}, {buffers + i * bytes, bytes}, 0, &tags[i]);
      accepted[i] = status.ok();
      rejected += !status.ok();
      passed &= tags[i].calls == 0;
    }
    Completion closed;
    passed &= !worker.SubmitCloseDirect({0}, &closed).ok();
    Drain(worker, [&] { return delivered == count - rejected; });
    unsigned completed = 0;
    for (unsigned i = 0; i < count; ++i) {
      if (!accepted[i]) continue;
      const bool valid = tags[i].calls == 1 && tags[i].result == bytes &&
                         std::memcmp(buffers + i * bytes,
                                     buffers + (i % files) * bytes, bytes) == 0;
      passed &= valid;
      completed += valid;
    }
    passed &= rejected == 0 && completed == count;
    std::cout << "round=" << round << " submitted=" << count
              << " rejected=" << rejected << " completed=" << completed << '\n';
  }
  for (unsigned i = 0; i < files; ++i) {
    Completion closed;
    passed &= worker.SubmitCloseDirect({i}, &closed).ok();
    Drain(worker, [&] { return closed.calls != 0; });
    passed &= closed.calls == 1 && closed.result == 0;
  }
  // Shutdown must finish an accepted read under the same worker TLS before
  // its caller-owned buffer and completion tag are reclaimed.
  Completion reopened;
  if (!worker.SubmitOpenDirect(argv[1], O_RDWR, 0, {0}, &reopened).ok())
    return 1;
  Drain(worker, [&] { return reopened.calls != 0; });
  passed &= reopened.calls == 1 && reopened.result == 0;
  Completion shutdown;
  if (!worker.SubmitRead({0}, {buffers, bytes}, 0, &shutdown).ok()) return 1;
  passed &= shutdown.calls == 0;
  worker.Shutdown();
  passed &= shutdown.calls == 1 && shutdown.result == bytes;
  bycorf::FreeStorageBuffer(buffers, bytes);
  close(wake_fd);
  std::cout << (passed ? "PASS" : "FAIL") << " SPDK request backpressure\n";
  return passed ? 0 : 1;
}
