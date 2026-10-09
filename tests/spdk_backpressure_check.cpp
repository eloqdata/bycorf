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
  void Complete(bycorf::Worker&, int value, unsigned) override {
    ++calls;
    result = value;
  }
};

void Drain(bycorf::SpdkStorageBackend& backend) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (backend.HasOutstanding()) {
    backend.Poll(16);
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
  bycorf::Worker worker;
  bycorf::SpdkStorageBackend backend;
  const unsigned files = argc - 1;
  if (!backend.Init(&worker).ok() || !backend.RegisterFixedFiles(files).ok())
    return 1;
  for (unsigned i = 0; i < files; ++i) {
    Completion opened;
    const auto status =
        backend.SubmitOpenDirect(argv[i + 1], O_RDWR, 0, {i}, &opened);
    if (!status.ok()) {
      std::cerr << status << '\n';
      return 1;
    }
    if (opened.calls != 0) return 1;
    Drain(backend);
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
    for (unsigned i = 0; i < count; ++i) {
      const auto status = backend.SubmitRead(
          {i % files}, {buffers + i * bytes, bytes}, 0, &tags[i]);
      accepted[i] = status.ok();
      rejected += !status.ok();
      passed &= tags[i].calls == 0;
    }
    Completion closed;
    passed &= !backend.SubmitCloseDirect({0}, &closed).ok();
    Drain(backend);
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
  bycorf::FreeStorageBuffer(buffers, bytes);
  for (unsigned i = 0; i < files; ++i) {
    Completion closed;
    passed &= backend.SubmitCloseDirect({i}, &closed).ok();
    Drain(backend);
    passed &= closed.calls == 1 && closed.result == 0;
  }
  backend.Shutdown();
  std::cout << (passed ? "PASS" : "FAIL") << " SPDK request backpressure\n";
  return passed ? 0 : 1;
}
