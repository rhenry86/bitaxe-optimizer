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
