import uvicorn
import server
import watchdog_patch
import reconnect_patch
import diagnostics_patch
import ui_patch

# Apply UI and reliability hardening before FastAPI startup creates miner tasks.
ui_patch.install()
watchdog_patch.install(server)
reconnect_patch.install(server)
diagnostics_patch.install(server)

if __name__ == "__main__":
    uvicorn.run(server.app, host="0.0.0.0", port=8080)
