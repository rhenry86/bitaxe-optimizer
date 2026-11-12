# Bitaxe Optimizer — Project State

**Documentation baseline:** 2026-10-30 Pacific. Existing public GitHub README read; Umbrel/miners runtime **not verified in this documentation pass**.

## Project

- Repo: `rhenry86/bitaxe-optimizer` (public Umbrel community app store).
- Runs an optimizer for Bitaxe / NerdQaxe / Octaxe family miners via AxeOS API.
- Existing README describes a normalized coupled frequency-voltage operating axis, ASIC and fan targets, HW errors, rejected shares and efficiency optimization.
- Published app is distributed by GHCR / Umbrel community store; `.github` workflows and app folder exist.

## Known recent user-reported issues

- Connected Bitaxes may disconnect / remain marked paused, and fail to reconnect, leaving optimization ineffective or unsafe.
- User requested an **automatic reconnect toggle** and a way to inspect all miners' actual telemetry remotely alongside optimizer-reported targets.
- Earlier configuration discussion included ASIC ~55 C, VRM ~65 C, min/max bands, hardware/reject thresholds and chip-outlier restart. **These are prior preferences, not a verified current build specification.**
- A remote read-only monitoring integration was discussed, but its existence/production readiness is not established by this repository snapshot.

## Safety and next steps

1. Review actual source and recent commit/workflow status, confirm deployed app version on Umbrel.
2. Reproduce lost-connection and stuck-paused behavior; add tests for reconnection and graceful degradation.
3. Verify full miner telemetry vs controller output with user-approved secure, read-only integration.
4. Document verified code changes, tests, GHCR publication and safe rollback before deploying to hardware.

## References

- `README.md`, `app/`, `rhenry86-bitaxe-optimizer/`, `.github/`
- `AGENTS.md` and `WORK_LOG.md` for required 30-minute active-work checkpoints.

## 2026-11-12 — Zero-hashrate recovery

- The 3.4.1 source adds a watchdog to the normal AxeOS telemetry poll. It detects a full-miner stall independently of chip logs and sends an AxeOS reboot after 60 seconds of observed zero hashrate while the optimizer is active.
- Missing telemetry, new accepted shares, pause, thermal limits and an active chip recovery interrupt detection. Recent miner startup defers a restart. A restart attempt starts a 60-second settling period and a five-minute per-miner retry cooldown.
- Local checks used simulated telemetry and a reboot response interrupted by a dropped connection. The diagnostics endpoint returned HTTP 530. Umbrel and physical miner recovery remain unverified.
- Publish via the existing Docker workflow and apply the 3.4.1 update in Umbrel. Roll back to the previously installed image digest if recovery regresses.
