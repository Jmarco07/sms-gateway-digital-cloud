"""FastAPI application: tenant API, DLR callback and admin API."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.admin import router as admin_router
from app.api_v1 import router as v1_router
from app.config import get_settings
from app.errors import ApiError
from app.state import build_state


def _field(loc: tuple) -> str:
    parts = [str(p) for p in loc if p != "body"]
    out = ""
    for p in parts:
        out += f"[{p}]" if p.isdigit() else (f".{p}" if out else p)
    return out or "body"


def create_app(settings=None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.gw = await build_state(settings)
        yield
        await app.state.gw.close()

    app = FastAPI(title="Mini SMS Gateway", version="1.0", lifespan=lifespan)

    @app.exception_handler(ApiError)
    async def api_error_handler(_: Request, exc: ApiError):
        return JSONResponse(exc.body(), status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError):
        details = [{"field": _field(e["loc"]), "message": e["msg"]} for e in exc.errors()]
        return JSONResponse(
            ApiError(422, "VALIDATION_ERROR", "request is invalid", details).body(), status_code=422)

    app.include_router(v1_router)
    app.include_router(admin_router)
    return app


app = create_app()
