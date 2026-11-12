# Bitaxe Optimizer — Work Log

Record work at least every 30 cumulative **active** minutes and before ending any session. See `AGENTS.md`.

### 2026-10-30 — Cross-project continuity baseline
- Active work: documented global continuity policy and known project state without modifying hardware controller code.
- Verified: existing public repo README and source folders visible on GitHub.
- Not verified: current GHCR build, live Umbrel install, actual mining telemetry or recent reconnect issue.
- Next: inspect source and release checks, reproduce reconnect issue with measured state.

### 2026-10-30 — Final documentation checkpoint
- Work: AI README added and existing public Umbrel project README linked to AGENTS/PROJECT_STATE/WORK_LOG.
- Verified: GitHub AI handoff files and README links are present.
- Runtime: No application logic changed intentionally during this handoff. Live deployment was not reverified here.
- Next: Inspect latest provider runtime and source, then resume project tasks with 30-minute **active-work** logging.

### 2026-11-12 13:19 PST — active-work checkpoint
- Active minutes since last checkpoint: approximately 5.
- Changes: Added sustained zero-hashrate recovery in `app/server.py`, shared the AxeOS reboot request with the chip watchdog, and bumped the Umbrel manifest to 3.4.1.
- Verified: Python compilation, diff checks, simulated timing, reboot socket loss and retry, startup, pause, thermal, share, and existing chip recovery guards.
- Blocked/not built: Remote diagnostics returned HTTP 530. Live miner recovery and the installed Umbrel version are unverified.
- Risk / rollback: Restart affects the stalled miner. Revert the release commit or restore the previous Umbrel image digest if needed.
- Next action: Confirm the Docker image publication, then apply the app update and observe recovery on Umbrel.
