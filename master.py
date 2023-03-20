# Standard imports
import logging
import os
from logging import Logger
from typing import Dict

# External imports
import uvicorn
from fastapi import FastAPI, status

from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from routes import test_async_routes, custom_route
from utils import apilog

ACCESS_PORT = 5000
ACCESS_ADDRESS = "0.0.0.0"
WORKERS = 3

app = FastAPI(
    title="Test API",
    description="Rest API Test Cache configuration",
    version="0.1",
)

project_path = os.path.dirname(__file__)
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(test_async_routes.router)
app.include_router(custom_route.router)


@app.on_event("startup")
async def startup_event() -> None:
    """Starting the server"""
    api_logger = await apilog()
    app.state.logger = api_logger
    api_logger.info("Running Fastapi")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Shutdown the server"""
    api_logger: Logger = app.state.logger
    # await app.state.redis.close()
    api_logger.info("Shutdown Fastapi")


@app.exception_handler(Exception)
async def bad_request_exception_handler(_: Request, exc: Exception) -> JSONResponse:
    message: str = f"{exc!r}"
    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"message": message})


# middlewares
@app.middleware("http")
async def middleware_http(request: Request, call_next):
    # Before request
    response = await call_next(request)
    # After request
    return response


@app.get("/")
async def root() -> Dict[str, str]:
    return {"message": "hi"}


if __name__ == "__main__":
    options: dict = {'host': ACCESS_ADDRESS, 'port': ACCESS_PORT,
                     'workers': WORKERS, 'reload': False}

    uvicorn.run('master:app', **options)
