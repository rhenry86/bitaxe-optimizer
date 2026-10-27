"""Read-only live diagnostics for Bitaxe Optimizer.

Compares fresh AxeOS telemetry with optimizer intent/state and records a short
history of F/V commands so remote diagnostics can distinguish transient settling
from a stale or runaway miner.
"""
import time
from collections import deque

import httpx

import reconnect_patch


def _same_host(server, a, b):
    try:
        return server.clean_host(a) == server.clean_host(b)
    except Exception:
        return str(a) == str(b)


def _find_miner_by_host(server, host):
    for mid, m in server.miners.items():
        if _same_host(server, m.get("host", ""), host):
            return mid, m
    return None, None


def _event(m, kind, **data):
    q = m.setdefault("diagnostic_events", deque(maxlen=100))
    q.append({"ts": time.time(), "kind": kind, **data})


def _task_state(server, mid):
    task = server.tasks.get(mid)
    if task is None:
        return "missing"
    if task.cancelled():
        return "cancelled"
    if task.done():
        try:
            exc = task.exception()
        except Exception as e:
            exc = e
        return "failed" if exc else "finished"
    return "running"


def _flags(server, mid, m, actual, desired):
    flags = []
    sev = "ok"

    def add(code, message, severity="warning"):
        nonlocal sev
        flags.append({"code": code, "severity": severity, "message": message})
        if severity == "critical" or (severity == "warning" and sev == "ok"):
            sev = severity

    if not actual:
        add("miner_unreachable", "Fresh AxeOS telemetry request failed", "critical")
        return sev, flags

    cmd = m.get("last_commanded_fv") or {}
    af = actual.get("frequency")
    av = actual.get("voltage")
    cf = cmd.get("frequency")
    cv = cmd.get("voltage")
    age = time.time() - cmd.get("ts", 0) if cmd.get("ts") else None
    settle = float((m.get("settings") or {}).get("settle_seconds", 30))

    # Only call F/V a mismatch after the configured settle window.
    if cf is not None and af is not None and age is not None and age > settle + 5 and abs(af - cf) > 1.0:
        add("frequency_mismatch", f"AxeOS is at {af:.0f} MHz but optimizer last commanded {cf:.0f} MHz", "critical")
    if cv is not None and av is not None and age is not None and age > settle + 5 and abs(av - cv) > 2.0:
        add("voltage_mismatch", f"AxeOS is at {av:.0f} mV but optimizer last commanded {cv:.0f} mV", "critical")

    task_state = _task_state(server, mid)
    if desired.get("desired_running") and desired.get("enabled") and task_state != "running":
        add("optimizer_task_missing", f"Optimizer should be running but task state is {task_state}", "critical")
    if not desired.get("desired_running") and m.get("mode") not in ("paused", None):
        add("pause_state_mismatch", f"Desired state is paused but app mode is {m.get('mode')}")

    s = m.get("settings") or {}
    temp = actual.get("temp")
    vr = actual.get("vr_temp")
    fan = actual.get("fan")
    if temp is not None and temp >= s.get("max_asic_temp", 999):
        add("asic_overtemp", f"ASIC is {temp:.1f}°C, at/above hard max {s.get('max_asic_temp'):.1f}°C", "critical")
    elif temp is not None and temp > s.get("asic_temp_target", 999) + s.get("temp_deadband", 0):
        add("asic_above_target", f"ASIC is {temp:.1f}°C above target band")
    if vr is not None and vr >= s.get("max_vr_temp", 999):
        add("vrm_overtemp", f"VRM is {vr:.1f}°C, at/above hard max {s.get('max_vr_temp'):.1f}°C", "critical")
    if fan is not None and fan > s.get("fan_ceiling", 100):
        add("fan_above_ceiling", f"Fan is {fan:.0f}% above configured ceiling {s.get('fan_ceiling'):.0f}%")

    if int(actual.get("reject_delta") or 0) > 0:
        add("new_rejects", f"{int(actual.get('reject_delta') or 0)} new rejected share(s) in app telemetry window")
    if int(actual.get("asic_count") or 1) < 4 and actual.get("error_pct", 0) > s.get("max_error_pct", 3):
        add("hw_errors_high", f"HW errors {actual.get('error_pct', 0):.2f}% exceed {s.get('max_error_pct', 3):.2f}%")

    wd = m.get("chip_watchdog") or {}
    if int(actual.get("asic_count") or 1) >= 4 and not wd.get("connected"):
        add("watchdog_disconnected", f"ASIC watchdog is not connected ({wd.get('status', 'unknown')})")

    return sev, flags


async def _fresh(server, m):
    try:
        raw = await server.getj(m["host"], "/api/system/info")
        return server.norm(raw, None)
    except Exception:
        return None


async def _diagnose_one(server, mid, m):
    actual = await _fresh(server, m)
    desired = reconnect_patch._get_state(server, mid)
    severity, flags = _flags(server, mid, m, actual, desired)
    cmd = dict(m.get("last_commanded_fv") or {})
    wd = dict(m.get("chip_watchdog") or {})
    events = list(m.get("diagnostic_events") or [])[-20:]
    return {
        "id": mid,
        "name": m.get("name"),
        "host": m.get("host"),
        "health": severity,
        "online_cached": bool(m.get("online")),
        "fresh_reachable": actual is not None,
        "desired": desired,
        "optimizer": {
            "mode": m.get("mode"),
            "reason": m.get("reason"),
            "task_state": _task_state(server, mid),
            "stability_trim_mv": m.get("stability_trim", 0),
            "last_optimizer_error": m.get("last_optimizer_error"),
        },
        "commanded": cmd,
        "actual": actual,
        "targets": {
            "asic_temp_c": (m.get("settings") or {}).get("asic_temp_target"),
            "vrm_temp_c": (m.get("settings") or {}).get("vr_temp_target"),
            "fan_pct": (m.get("settings") or {}).get("fan_target"),
            "fan_ceiling_pct": (m.get("settings") or {}).get("fan_ceiling"),
            "strategy": (m.get("settings") or {}).get("efficiency_strategy"),
        },
        "watchdog": wd,
        "flags": flags,
        "recent_commands": events,
        "checked_at": time.time(),
    }


def install(server):
    original_patch = server.patch

    async def tracked_patch(host, data):
        result = await original_patch(host, data)
        mid, m = _find_miner_by_host(server, host)
        if m is not None:
            if "frequency" in data or "coreVoltage" in data:
                prev = m.get("last_commanded_fv") or {}
                f = data.get("frequency", prev.get("frequency"))
                v = data.get("coreVoltage", prev.get("voltage"))
                m["last_commanded_fv"] = {
                    "frequency": float(f) if f is not None else None,
                    "voltage": float(v) if v is not None else None,
                    "ts": time.time(),
                    "source": "optimizer_api_patch",
                }
                _event(m, "fv_command", frequency=f, voltage=v)
            if "autofanspeed" in data:
                _event(m, "fan_command", autofanspeed=data.get("autofanspeed"))
        return result

    server.patch = tracked_patch

    @server.app.get("/api/diagnostics")
    async def diagnostics_all():
        rows = []
        for mid, m in list(server.miners.items()):
            rows.append(await _diagnose_one(server, mid, m))
        overall = "ok"
        if any(x["health"] == "critical" for x in rows):
            overall = "critical"
        elif any(x["health"] == "warning" for x in rows):
            overall = "warning"
        return {"overall": overall, "miners": rows, "checked_at": time.time()}

    @server.app.get("/api/diagnostics/{mid}")
    async def diagnostics_one(mid: int):
        if mid not in server.miners:
            from fastapi import HTTPException
            raise HTTPException(404)
        return await _diagnose_one(server, mid, server.miners[mid])
