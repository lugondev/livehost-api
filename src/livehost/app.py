from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from livehost.api.control import router as control_router
from livehost.api.ws import router as ws_router

_STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="livehost-api")
app.include_router(ws_router)
app.include_router(control_router)
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


@app.get("/ui")
async def ui() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")
