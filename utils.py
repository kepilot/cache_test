import asyncio
import json
import logging
import sys
import time
import uuid
from datetime import timedelta
from typing import List, Dict, Any, cast

from cache.decorators import cached, TestCache
from cache.test_cache import SerializableData
from models.user import User


async def apilog():
    process_logger: logging.Logger = logging.getLogger("test")
    process_logger.propagate = False
    process_logger.setLevel(logging.INFO)
    log_format: str = "%(asctime)s %(levelname)s func:%(funcName)s line:%(lineno)d - %(message)s"

    log_formatter: logging.Formatter = logging.Formatter(log_format)
    log_stream_formatter = logging.Formatter(log_format)
    file_handler: logging.FileHandler = logging.FileHandler(
        filename="test.log", mode="w", encoding="utf-8"
    )
    console_handler = logging.StreamHandler(stream=sys.stdout)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(log_stream_formatter)
    process_logger.addHandler(file_handler)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(log_stream_formatter)
    process_logger.addHandler(console_handler)
    return process_logger


# async_ def async_create_event(times, software_type, cache_type, start, end):
#     elapsed = (end - start).microseconds
#     event: Dict[str, Any] = {"times": times, software_type: cache_type, "start": start.isoformat(),
#                              "end": end.isoformat(),
#                              "elapsed": f"{elapsed / 1000} miliseconds"}
#     return event

def create_event(times, software_type, cache_type, data, start, end):
    delta: timedelta = (end - start)
    elapsed = delta.seconds + (delta.microseconds / 1_000_000)

    event: Dict[str, Any] = {"times": times, software_type: cache_type, "start": start.isoformat(),
                             "end": end.isoformat(), "elapsed": f"{elapsed} seconds",
                             "data": data,
                             }
    return event


def create_user(username: str) -> User:
    uid: str = str(uuid.uuid4())
    claims: dict = {"Hola": "Hola"}
    permissions: list = ["Adios", "bye"]
    user_details: dict = {"details": "details"}
    token: str = "jwt"
    user = User(username=username, uid=uid, claims=claims, permissions=permissions,
                user_details=user_details, token=token)
    return user


async def async_call_helper(username) -> User:
    cache: TestCache = TestCacheCreate.create("memory")
    value: User
    value = cast(User, await cache.get(username))
    if not value:
        value = cast(User, get_sync_data(username))
        _: bool = await cache.put(username, value)
        print("Async helper Function valor NO cacheado", value)
    # _: bool = fwk_cache.close()
    else:
        print("Async helper Function valor cacheado", value)
    return value


def sync_call_helper(username) -> User:
    cache: TestCache = TestCacheCreate.create("memory")
    value: User
    value = cast(User, cache.get(username))
    if not value:
        value = cast(User, get_sync_data(username))
        _: bool = cache.put(username, value)
        print("Sync helper Function valor NO cacheado", value)
    # _: bool = fwk_cache.close()
    else:
        print("Sync helper Function valor cacheado", value)
    return value


# @cached("redis")
def get_sync_data(username: str) -> User:
    # print("Creando Usuario Sincrono")
    time.sleep(0)
    value: User = create_user(username)
    return value


# @cached("memory")
async def get_async_data(username: str) -> User:
     # print("Creando Usuario Asincrono")
    await asyncio.sleep(0)
    value: User = create_user(username)
    return value




# print("Creando Usuario Sincrono")

def test(value: int) -> int:
    time.sleep(value)
    return 0


async def async_test(value: int) -> int:
    await asyncio.sleep(value)
    return 0
