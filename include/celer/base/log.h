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

#ifndef CELER_BASE_LOG_H_
#define CELER_BASE_LOG_H_

#include <sstream>
#include <string_view>

namespace celer {
namespace log {

enum class Level {
  kInfo,
  kWarn,
  kError,
};

class LogMessage {
 public:
  LogMessage(Level level, const char* file, int line);
  ~LogMessage();

  template <typename T> LogMessage& operator<<(const T& value) {
    stream_ << value;
    return *this;
  }

 private:
  Level level_;
  const char* file_;
  int line_;
  std::ostringstream stream_;
};

}  // namespace log
}  // namespace celer

#define CELER_LOG_INFO ::celer::log::LogMessage(::celer::log::Level::kInfo, __FILE__, __LINE__)
#define CELER_LOG_WARN ::celer::log::LogMessage(::celer::log::Level::kWarn, __FILE__, __LINE__)
#define CELER_LOG_ERROR \
  ::celer::log::LogMessage(::celer::log::Level::kError, __FILE__, __LINE__)

#endif  // CELER_BASE_LOG_H_
