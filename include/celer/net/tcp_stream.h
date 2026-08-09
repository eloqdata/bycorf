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

#ifndef CELER_NET_TCP_STREAM_H_
#define CELER_NET_TCP_STREAM_H_

#include <cstddef>
#include <span>

#include "absl/status/statusor.h"
#include "celer/net/connection.h"
#include "celer/runtime/task.h"

namespace celer {

class TcpStream {
 public:
  TcpStream() = default;
  explicit TcpStream(Connection* connection) : connection_(connection) {}

  TcpStream(TcpStream&&) noexcept = default;
  TcpStream& operator=(TcpStream&&) noexcept = default;

  TcpStream(const TcpStream&) = delete;
  TcpStream& operator=(const TcpStream&) = delete;

  bool IsOpen() const noexcept;
  int NativeFd() const noexcept;

  Task<absl::StatusOr<std::size_t>> ReadSome(std::span<std::byte> buffer);
  Task<absl::StatusOr<std::size_t>> WriteSome(
      std::span<const std::byte> buffer);
  Task<absl::Status> WriteAll(std::span<const std::byte> buffer);

  absl::Status Close() noexcept;

 private:
  Connection* connection_ = nullptr;
};

}  // namespace celer

#endif  // CELER_NET_TCP_STREAM_H_
