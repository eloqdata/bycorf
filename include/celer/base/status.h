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

#ifndef CELER_BASE_STATUS_H_
#define CELER_BASE_STATUS_H_

#include <string>
#include <utility>

namespace celer {

enum class StatusCode {
  kOk = 0,
  kCancelled,
  kUnknown,
  kInvalidArgument,
  kDeadlineExceeded,
  kNotFound,
  kAlreadyExists,
  kPermissionDenied,
  kResourceExhausted,
  kFailedPrecondition,
  kAborted,
  kOutOfRange,
  kUnimplemented,
  kInternal,
  kUnavailable,
};

class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }

  bool ok() const noexcept { return code_ == StatusCode::kOk; }
  StatusCode code() const noexcept { return code_; }
  const std::string& message() const noexcept { return message_; }

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

template <typename T>
class StatusOr {
 public:
  StatusOr(const T& value) : value_(value), has_value_(true) {}
  StatusOr(T&& value) : value_(std::move(value)), has_value_(true) {}
  StatusOr(const Status& status) : status_(status) {}
  StatusOr(Status&& status) : status_(std::move(status)) {}

  bool ok() const noexcept { return has_value_; }
  const Status& status() const noexcept { return has_value_ ? ok_status_ : status_; }

  const T& value() const& { return value_; }
  T& value() & { return value_; }
  T&& value() && { return std::move(value_); }

  const T& operator*() const& { return value_; }
  T& operator*() & { return value_; }
  T* operator->() { return &value_; }
  const T* operator->() const { return &value_; }

 private:
  inline static const Status ok_status_ = Status::Ok();

  T value_{};
  Status status_ = Status::Ok();
  bool has_value_ = false;
};

}  // namespace celer

#endif  // CELER_BASE_STATUS_H_
