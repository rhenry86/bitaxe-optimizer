"""Persistent reconnect/resume behavior for Bitaxe Optimizer.

The original app stores miners/settings, but optimizer run state lives only in
memory.  A container/app restart therefore reloads every miner as `paused` and
never recreates the optimizer task.  This module keeps a tiny persistent desired
state and supervises optimizer tasks when auto-reconnect is enabled.
"""
import asyncio
import time
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel


class ReconnectIn(BaseModel):
    enabled: bool


def _ensure_table(server):
    c = server.con()
    c.execute(
        """CREATE TABLE IF NOT EXISTS reconnect_state(
               miner_id INTEGER PRIMARY KEY,
               enabled INTEGER NOT NULL DEFAULT 1,
               desired_running INTEGER NOT NULL DEFAULT 0
           )"""
    )
    # Existing and newly-created miners default to auto reconnect ON.
    c.execute(
        """INSERT OR IGNORE INTO reconnect_state(miner_id,enabled,desired_running)
           SELECT id,1,0 FROM miners"""
    )
    c.execute(
        "DELETE FROM reconnect_state WHERE miner_id NOT IN (SELECT id FROM miners)"
    )
    c.commit()
    c.close()


def _get_state(server, mid):
    _ensure_table(server)
    c = server.con()
    row = c.execute(
        "SELECT enabled,desired_running FROM reconnect_state WHERE miner_id=?", (mid,)
    ).fetchone()
    c.close()
    if row is None:
        return {"enabled": True, "desired_running": False}
    return {"enabled": bool(row["enabled"]), "desired_running": bool(row["desired_running"])}


def _set_state(server, mid, *, enabled=None, desired_running=None):
    if mid not in server.miners:
        raise HTTPException(404)
    _ensure_table(server)
    state = _get_state(server, mid)
    if enabled is not None:
        state["enabled"] = bool(enabled)
    if desired_running is not None:
        state["desired_running"] = bool(desired_running)
    c = server.con()
    c.execute(
        """INSERT INTO reconnect_state(miner_id,enabled,desired_running)
           VALUES(?,?,?)
           ON CONFLICT(miner_id) DO UPDATE SET
             enabled=excluded.enabled,
             desired_running=excluded.desired_running""",
        (mid, int(state["enabled"]), int(state["desired_running"])),
    )
    c.commit()
    c.close()
    return state


async def _supervisor(server):
    """Keep desired optimizer tasks alive without overriding an intentional Pause."""
    while True:
        try:
            _ensure_table(server)
            for mid, miner in list(server.miners.items()):
                state = _get_state(server, mid)
                task = server.tasks.get(mid)

                if state["enabled"] and state["desired_running"]:
                    if task is None or task.done():
                        # If the previous task crashed, surface that briefly, then recover.
                        if task is not None and task.done() and not task.cancelled():
                            try:
                                exc = task.exception()
                            except Exception:
                                exc = None
                            if exc:
                                miner["last_optimizer_error"] = str(exc)
                        miner["reason"] = "Auto reconnect: restoring optimizer control"
                        server.tasks[mid] = asyncio.create_task(server.optimize(mid))
                elif not state["desired_running"]:
                    # Intentional Pause is authoritative. Do not auto-start.
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            # Supervisor itself must stay alive; retry on the next pass.
            pass
        await asyncio.sleep(3)


def install(server):
    _ensure_table(server)

    @server.app.on_event("startup")
    async def reconnect_startup():
        _ensure_table(server)
        server._reconnect_supervisor_task = asyncio.create_task(_supervisor(server))

    @server.app.on_event("shutdown")
    async def reconnect_shutdown():
        task = getattr(server, "_reconnect_supervisor_task", None)
        if task:
            task.cancel()

    @server.app.get("/api/miners/{mid}/reconnect-state")
    async def reconnect_state(mid: int):
        if mid not in server.miners:
            raise HTTPException(404)
        return _get_state(server, mid)

    @server.app.put("/api/miners/{mid}/reconnect-state")
    async def set_reconnect_state(mid: int, body: ReconnectIn):
        return _set_state(server, mid, enabled=body.enabled)

    @server.app.post("/api/miners/{mid}/managed-optimize")
    async def managed_optimize(mid: int):
        state = _set_state(server, mid, desired_running=True)
        task = server.tasks.get(mid)
        if task is None or task.done():
            server.tasks[mid] = asyncio.create_task(server.optimize(mid))
        server.miners[mid]["reason"] = (
            "Optimizer enabled — auto reconnect active"
            if state["enabled"]
            else "Optimizer enabled — auto reconnect disabled"
        )
        return {"running": True, **state}

    @server.app.post("/api/miners/{mid}/managed-stop")
    async def managed_stop(mid: int):
        state = _set_state(server, mid, desired_running=False)
        task = server.tasks.pop(mid, None)
        if task:
            task.cancel()
        server.miners[mid]["mode"] = "paused"
        server.miners[mid]["reason"] = "Optimizer paused by user"
        return {"running": False, **state}

    @server.app.get("/reconnect.js")
    async def reconnect_js():
        p = Path(__file__).parent / "reconnect.js"
        return Response(p.read_text(), media_type="application/javascript")

    @server.app.middleware("http")
    async def inject_reconnect_ui(request, call_next):
        if request.url.path == "/" and request.method == "GET":
            p = Path(server.__file__).parent / "static" / "index.html"
            html = p.read_text()
            if "/reconnect.js" not in html:
                html = html.replace("</body>", '<script src="/reconnect.js"></script></body>')
            return HTMLResponse(html)
        return await call_next(request)
