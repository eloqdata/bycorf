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

# RPC

The `bycorf::rpc` target provides the repository's RPC client/server layer above
Bycorf's TCP streams. It owns framing, request dispatch, and reply correlation;
it relies on the runtime and networking modules for scheduling, connection
lifetime, and transport completion. Applications that only need raw streams or
a different wire protocol link the core target without RPC.

Sources: `include/bycorf/rpc/`, `src/rpc/rpc.cpp`,
`example/rpc_echo_server.cpp`, `example/rpc_bench.cpp`.
