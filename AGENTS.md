# AI Agent Operating Instructions

**Read this before ANY work.** Then read `PROJECT_STATE.md`, the latest `WORK_LOG.md`, and this repository's existing `README.md` and source files. These committed files are the cross-chat handoff, not conversation memory.

## Universal policy — requested 2026-10-30 (all projects)

1. **Checkpoint at least every 30 minutes of cumulative actual active work** (building, researching, debugging, testing, deployment, integration). **Do not** treat idle/sleep/waiting time as active work. Track active work within the session; write to `WORK_LOG.md` with the date/time and work minutes, update `PROJECT_STATE.md`, and commit. If a meaningful event occurs sooner, checkpoint sooner.
2. **Always checkpoint before ending a session**, changing projects/agents/chats, or risky deployment, even if fewer than 30 minutes elapsed. Record decisions, code paths, tests, live deploy/environment version, verified vs unverified status, blockers, rollback and the next action. Do not leave critical progress only in the chat.
3. At the next session, **read state/log and verify current provider state** before making assumptions, since the repo may lag live configuration. If tools/permissions are missing, identify the missing capability explicitly; do not claim completed work.
4. **Protect secrets and personal data:** never commit API tokens, passwords, credentials, webhooks with private tokens, customer contacts, sensitive records, account numbers, private invoices, private transaction exports, or raw logs with credentials. GitHub private visibility is not a secret store.
5. Prefer **reproducible source-controlled changes**, verification and a rollback plan. Check tests, mobile responsiveness, real integration behavior and relevant edge cases; report actual observed results.
6. This policy guides active AI sessions; it **does not** itself schedule future ChatGPT work, automatically checkpoint during periods with no active agent, or create a background process.
7. Maintain accurate customer-facing claims; avoid enabling unsolicited marketing, payments, external messages, irreversible production changes, or data collection without applicable authorization/compliance and suitable checks.
8. **Protect unrelated projects** and never reuse credentials/customer data across projects without explicit authority.

## Required work-log format

```md
### YYYY-MM-DD HH:MM TZ — active-work checkpoint
- Active minutes since last checkpoint: approximately N; idle excluded
- Changes: [what/where]
- Verified: [test, link, deployment or commit SHA]
- Blocked/not built: [explicitly]
- Risk / rollback: [if relevant]
- Next action: [specific task]
```

At session end, update `PROJECT_STATE.md` and `WORK_LOG.md` and commit, even if this is before 30 active minutes.

## Project-specific care: Bitaxe Optimizer

- Public Umbrel community app controlling real ASIC mining hardware. Thermal, voltage/frequency and auto-restart changes can have physical consequences; test conservatively.
- Known historical issue: connected Bitaxes can disappear while miner remains paused; verify reconnect behavior and controller state before changing. Do not deploy guess-based controls.
- Follow actual repo controller design: existing README documents coupled normalized frequency/voltage axis and AxeOS auto fan fallback. Historical ideas may differ from current committed logic; inspect source and hardware state.
- Never commit miner URLs on a private LAN, pool credentials or telemetry with personal identifiers.