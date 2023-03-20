import asyncio
import inspect
from types import FrameType
from typing import Any, Union, Callable, cast

from cache.test_memorycache import AsyncMemoryCache, SyncMemoryCache
from cache.test_rediscache import SyncRedisCache, AsyncRedisCache
from cache.test_cache import TestCache, Backend

TestCacheInstance = Union[AsyncMemoryCache, AsyncRedisCache, SyncMemoryCache, SyncRedisCache]


class TestCacheCreate:
    def __init__(self):
        raise TypeError("This class is not instantiable use create method for the object creation")

    @staticmethod
    def create(alias: str) -> TestCacheInstance:
        backend_type: str = TestCache.get_backend_type(alias)
        try:
            current_frame: FrameType = cast(FrameType, inspect.currentframe())
            function_name: str = cast(FrameType, current_frame.f_back).f_code.co_name
            function_: Callable[[..., Any], Any] = current_frame.f_back.f_globals[function_name]
            if asyncio.iscoroutinefunction(function_):
                if backend_type == Backend.REDIS:
                    return AsyncRedisCache(alias)
                elif backend_type == Backend.MEMORY:
                    return AsyncMemoryCache(alias)
                else:
                    raise NotImplementedError(f"The backend type {backend_type} is not implemented")
            else:
                if backend_type == Backend.REDIS:
                    return SyncRedisCache(alias)
                elif backend_type == Backend.MEMORY:
                    return SyncMemoryCache(alias)
                else:
                    raise NotImplementedError(f"The backend type {backend_type} is not implemented")

        except (RuntimeError, TypeError, KeyError):
            if backend_type == Backend.REDIS:
                return SyncRedisCache(alias)
            elif backend_type == Backend.MEMORY:
                return SyncMemoryCache(alias)
            else:
                raise NotImplementedError(f"The backend type {backend_type} is not implemented")

