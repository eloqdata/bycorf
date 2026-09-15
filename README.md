<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# celer

Linux C++23 coroutine runtime with io_uring, optional SPDK storage, and an
optional native FreeBSD/DPDK TCP backend.

- [Architecture](docs/architecture/README.md)
- [DPDK prototype build and tests](docs/dpdk-prototype.md)

## Development

Celer uses Google-style C++23 formatting and pins clang-format 23.1.0 through
`pre-commit`. Install and enable the hook in a standalone Celer checkout:

```bash
sudo apt-get install pre-commit
pre-commit install
```

The hook formats staged first-party C and C++ files. If it changes a file, the
commit stops so the result can be reviewed and staged before retrying. Format
the complete maintained source tree with:

```bash
pre-commit run clang-format --all-files
```

The CMake `format` and `format-check` targets are enabled only when the detected
system clang-format reports exactly version 23.1.0. The pre-commit environment
downloads that pinned formatter independently, so it does not add a runtime
dependency to Celer.
