# README FOR AI — Bitaxe Optimizer

**Purpose:** A durable handoff independent of previous conversations. This repository is the source of truth only for committed source; verify the deployed Umbrel installation and physical miners separately.

At the start of every session, read [AGENTS.md](AGENTS.md), [PROJECT_STATE.md](PROJECT_STATE.md), the latest [WORK_LOG.md](WORK_LOG.md), and actual controller source/tests. Carefully check version, hardware limits, and the GHCR release path.

**Project care:** This optimizer controls real mining hardware. Reproduce failures, validate reconnect and thermal safety behavior and ensure rollback before publishing changes. Historical settings may conflict with committed controller logic.

Update `PROJECT_STATE.md` and `WORK_LOG.md` at least once per **30 minutes of cumulative active work**, excluding idle time. Also log and commit before switching projects or ending a session, even before 30 minutes. This is a procedure for active sessions, not autonomous background work.

Public repository: never commit miner LAN addresses, device access credentials, pools/account keys, customer/private data or security tokens.
