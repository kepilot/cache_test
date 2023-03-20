# standard imports

from datetime import datetime, timedelta

# external imports
from logging import Logger
from typing import Any, Dict, List, cast

from fastapi import APIRouter, Depends, Header, Request, status, Path
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse

import utils
from cache.DElete_cache_configuration import SerializableData
from cache.cache_creator import TestCacheCreate
from cache.decorators import TestCache
from cache.test_memorycache import AsyncMemoryCache
from cache.test_rediscache import AsyncRedisCache
from cache.test_cache import AbstractAsyncTestCache
from dependencies.api_log import get_log
from services import cache_system
from models.user import User

router = APIRouter(prefix="/test/async", tags=["api v1"])

TIMES: int = 10_000_000


@router.get(
    "/aiocache/{cache_type}",
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    responses={400: {"model": str}, 404: {"model": str}},
)
async def test_async_aiocache(request: Request, log: Logger = Depends(get_log),
                              cache_type: str = Path("memory", title="Cache")) -> JSONResponse:
    """Check synchronous Pipeline statusr"""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    try:
        print("===============================================================")
        fwk_cache: AbstractAsyncTestCache
        start = datetime.now()
        value: SerializableData = ""
        data: Dict[str, Any] = {}
        key_code: str = "async_code"
        key_decorator: str = "async_decorator"
        key_create: str = "async_create"
        cache: TestCache = TestCacheCreate.create("memory")
        value: User = cast(User, await cache.get(key_create))
        if not value:
            value: User = cast(User, await utils.get_async_data(key_create))
            _: bool = await cache.put(key_create, value)
            print("Async helper valor NO cacheado", value)
        else:
            print("Async helper valor cacheado", value)
            # print("close", await cache.close())
            # print("delete", await cache.delete(key_decorator))
        # print("exists", await cache.exists(key_decorator))
        _: bool = await cache.close()
        if cache_type == "redis":
            value = await cache_system.redis_get_data_async(key_decorator)
            data.update({"decorator": value})
            fwk_cache = AsyncRedisCache("redis")
            value: SerializableData = await fwk_cache.get(key_code)
            if not value:
                value = cast(User, await utils.get_async_data(key_code))
                _: bool = await fwk_cache.put(key_code, value)
                print("Async redis valor NO cacheado", value)
            else:
                print("Async redis valor cacheado", value)
        elif cache_type == "memory":
            value = await cache_system.memory_get_data_async(key_decorator)
            data.update({"decorator": value})
            fwk_cache: AsyncMemoryCache = AsyncMemoryCache("memory")
            # print("Exists the values ", await fwk_cache.exists(key_code))
            value: SerializableData = await fwk_cache.get(key_code)
            if not value:
                value = await utils.get_async_data(key_code)
                _: bool = await fwk_cache.put(key_code, value)
                print("Async memory valor NO cacheado", value)
            else:
                print("Async memory valor cacheado", value)
            # print("deleted ", await fwk_cache.delete(key_code))

        end = datetime.now()
        data.update({"code": value})
        print("===============================================================")
        body: Dict[str, Any] = jsonable_encoder(utils.create_event(1, "aiocache", cache_type, data, start, end))
        return JSONResponse(status_code=status.HTTP_200_OK, content=body, headers=headers)
    except Exception as exc:
        message: str = f"{exc!r}"
        log.exception("%s", message)
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"message": message})


@router.get(
    "/cachetools/{cache_type}",
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    responses={400: {"model": str}, 404: {"model": str}},
)
async def test_cachetools(request: Request, log: Logger = Depends(get_log),
                          cache_type: str = Path(..., title="Cache Type"),
) -> JSONResponse:
    """Check synchronous Pipeline statusr"""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    try:
        start = datetime.now()
        if cache_type == "redis":
            _: List[str] = cache_system.cachetools_redis_data(TIMES)
        else:
            _: List[str] = cache_system.cachetools_memlocal_data(TIMES)
        end = datetime.now()

        body: Dict[str, Any] = jsonable_encoder(await utils.create_event(TIMES, "cachetools",
                                                                         cache_type, start, end))
        return JSONResponse(status_code=status.HTTP_200_OK, content=body, headers=headers)
    except Exception as exc:
        message: str = f"{exc!r}"
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"message": message})


@router.get(
    "/beaker/{cache_type}",
    response_class=JSONResponse,
    status_code=status.HTTP_200_OK,
    responses={400: {"model": str}, 404: {"model": str}},
)
async def test_beaker(request: Request, log: Logger = Depends(get_log),
                      cache_type: str = Path(..., title="Cache Type"),
) -> JSONResponse:
    """Check synchronous Pipeline statusr"""
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
    }
    try:
        start = datetime.now()
        if cache_type == "redis":
            _: List[str] = cache_system.beaker_redis_data(TIMES)
        else:
            _: List[str] = cache_system.beaker_memlocal_data(TIMES)
        # red = await request.app.state.redis
        end = datetime.now()

        body: Dict[str, Any] = jsonable_encoder(await utils.create_event(TIMES, "beaker",
                                                                         cache_type, start, end))
        return JSONResponse(status_code=status.HTTP_200_OK, content=body, headers=headers)
    except Exception as exc:
        message: str = f"{exc!r}"
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"message": message})

