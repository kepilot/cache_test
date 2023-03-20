import asyncio
import inspect
from functools import wraps
from types import FrameType
from typing import Any, Mapping, Tuple, Union, Callable, cast

from cache.test_memorycache import AsyncMemoryCache, SyncMemoryCache
from cache.test_rediscache import SyncRedisCache, AsyncRedisCache
from cache.test_cache import SerializableData, TestCache, Backend

AsyncTestCacheInstance = Union[AsyncMemoryCache, AsyncRedisCache]
SyncTestCacheInstance = Union[SyncMemoryCache, SyncRedisCache]


def _key_from_args(func, args: Tuple[Any, ...], kwargs: Mapping[Any, Any]) -> str:
    ordered_kwargs: Any = sorted(kwargs.items())
    return str(func.__module__ or '') + func.__name__ + str(args) + str(ordered_kwargs)


def cached(alias: str) -> Any:

    def manage_params(func: Any) -> Any:
        @wraps(func)
        async def handled_async_func(*args, **kwargs) -> Any:
            backend_type: str = TestCache.get_backend_type(alias)
            if backend_type == Backend.REDIS:
                fwk_cache: AsyncRedisCache = AsyncRedisCache(alias)
            elif backend_type == Backend.MEMORY:
                fwk_cache: AsyncMemoryCache = AsyncMemoryCache(alias)
            else:
                raise NotImplementedError(f"The backend type {backend_type} is not implemented")
            key = _key_from_args(func, args, kwargs)
            value: SerializableData = await fwk_cache.get(key)
            if not value:
                value: SerializableData = await func(*args, **kwargs)
                _: bool = await fwk_cache.put(key, value)
                print(f"Async decorator {backend_type} valor NO cacheado", value)
            else:
                print(f"Async decorator {backend_type} valor cacheado", value)
            _: await fwk_cache.close()
            return value

        @wraps(func)
        def handled_sync_func(*args, **kwargs) -> Any:
            backend_type: str = TestCache.get_backend_type(alias)
            if backend_type == Backend.REDIS:
                fwk_cache: SyncRedisCache = SyncRedisCache(alias)
            elif backend_type == Backend.MEMORY:
                fwk_cache: SyncMemoryCache = SyncMemoryCache(alias)
            else:
                raise NotImplementedError(f"The backend type {backend_type} is not implemented")
            key = _key_from_args(func, args, kwargs)
            value: SerializableData = fwk_cache.get(key)
            if not value:
                value: SerializableData = func(*args, **kwargs)
                _: bool = fwk_cache.put(key, value)
                print(f"Sync decorator {backend_type} valor NO cacheado", value)
            else:
                print(f"Sync decorator {backend_type} valor cacheado", value)
            _: fwk_cache.close()
            return value

        if asyncio.iscoroutinefunction(func):
            return handled_async_func
        return handled_sync_func
    return manage_params
