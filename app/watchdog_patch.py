"""Runtime hardening for the AxeOS /api/ws chip watchdog.

Kept separate from server.py so the watchdog can be fixed without disturbing the
optimizer engine.  install(server) replaces the two small helpers used by the
existing chip_watchdog task.
"""
import asyncio
import struct
import re

MAX_WS_MESSAGE = 2 * 1024 * 1024


async def _read_exact(reader, n):
    return await reader.readexactly(n)


async def _read_frame_parts(reader):
    h = await _read_exact(reader, 2)
    fin = bool(h[0] & 0x80)
    opcode = h[0] & 0x0F
    masked = bool(h[1] & 0x80)
    n = h[1] & 0x7F

    if n == 126:
        n = struct.unpack("!H", await _read_exact(reader, 2))[0]
    elif n == 127:
        n = struct.unpack("!Q", await _read_exact(reader, 8))[0]

    if n > MAX_WS_MESSAGE:
        raise ConnectionError(f"WebSocket frame too large: {n} bytes")

    mask = await _read_exact(reader, 4) if masked else None
    payload = await _read_exact(reader, n) if n else b""
    if mask:
        payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return fin, opcode, payload


def install(server):
    async def robust_ws_read_frame(reader, writer):
        """Return one complete data message, assembling continuation frames.

        AxeOS is allowed to fragment a text/binary WebSocket message.  The old
        reader returned each continuation frame separately, which meant a
        `chip hashrates:` log line split across frames was silently missed.
        """
        while True:
            fin, opcode, payload = await _read_frame_parts(reader)

            if opcode == 0x9:  # ping
                await server._ws_send_pong(writer, payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode == 0x8:  # close
                raise ConnectionError("WebSocket closed")
            if opcode not in (0x1, 0x2):
                # Ignore stray continuation/control/reserved frames until a
                # new text or binary message starts.
                continue

            if fin:
                return payload

            message = bytearray(payload)
            while True:
                fin2, opcode2, payload2 = await _read_frame_parts(reader)
                if opcode2 == 0x9:
                    await server._ws_send_pong(writer, payload2)
                    continue
                if opcode2 == 0xA:
                    continue
                if opcode2 == 0x8:
                    raise ConnectionError("WebSocket closed")
                if opcode2 != 0x0:
                    raise ConnectionError(
                        f"Unexpected WebSocket opcode {opcode2} during fragmented message"
                    )
                message.extend(payload2)
                if len(message) > MAX_WS_MESSAGE:
                    raise ConnectionError("WebSocket message too large")
                if fin2:
                    return bytes(message)

    def robust_chip_rates_from_log(text):
        """Extract the newest complete 8-chip hashrate set from a log message."""
        if not text:
            return None

        # AxeOS log payloads can contain several lines/messages. Search all
        # occurrences and keep the newest complete 8-chip set.
        matches = list(
            re.finditer(
                r"chip\s+hashrates\s*:\s*(.*?)(?:\(\s*total\s*:|[\r\n]|$)",
                text,
                flags=re.IGNORECASE,
            )
        )
        for match in reversed(matches):
            vals = [
                float(x)
                for x in re.findall(
                    r"([0-9]+(?:\.[0-9]+)?)\s*GH/s", match.group(1), flags=re.IGNORECASE
                )
            ]
            if len(vals) == 8:
                return vals
        return None

    server._ws_read_frame = robust_ws_read_frame
    server._chip_rates_from_log = robust_chip_rates_from_log
