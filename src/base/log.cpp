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

#include "celer/base/log.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

namespace celer {
namespace log {

namespace {

const char* LevelName(Level level) {
  switch (level) {
    case Level::kInfo:
      return "INFO";
    case Level::kWarn:
      return "WARN";
    case Level::kError:
      return "ERROR";
  }
  return "UNKNOWN";
}

const char* BaseName(const char* file) {
  const char* slash = std::strrchr(file, '/');
  return slash == nullptr ? file : slash + 1;
}

}  // namespace

LogMessage::LogMessage(Level level, const char* file, int line)
    : level_(level), file_(file), line_(line) {
}

LogMessage::~LogMessage() {
  const auto now = std::chrono::system_clock::now();
  const auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
  const auto tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
  std::fprintf(stderr, "[%lld] [%s] [%zx] %s:%d %s\n",
               static_cast<long long>(millis), LevelName(level_),
               static_cast<std::size_t>(tid), BaseName(file_), line_,
               stream_.str().c_str());
  std::fflush(stderr);
}

}  // namespace log
}  // namespace celer

