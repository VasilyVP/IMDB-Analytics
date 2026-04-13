from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
#from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.database import close_db, get_duckdb, init_db
from app.core.startup import run_startup_actions


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    init_db()
    run_startup_actions(get_duckdb())
    yield
    close_db()


app = FastAPI(title="Movies API", version="0.1.0", lifespan=lifespan)

""" app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
) """

app.include_router(api_router)
