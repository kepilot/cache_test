import asyncio
from functools import wraps
from typing import Optional, Any, Mapping, Tuple

from cache.test_memorycache import AsyncMemoryCache, SyncMemoryCache
from cache.test_rediscache import SyncRedisCache, AsyncRedisCache
from cache.test_cache import SerializableData


def _key_from_args(func, args: Tuple[Any, ...], kwargs: Mapping[Any, Any]) -> str:
    ordered_kwargs: Any = sorted(kwargs.items())
    return str(func.__module__ or '') + func.__name__ + str(args) + str(ordered_kwargs)


def redis_cache(alias: str, ttl: Optional[int] = None,
                namespace: Optional[str] = None) -> Any:

    def manage_params(func: Any) -> Any:
        @wraps(func)
        async def handled_async_func(*args, **kwargs) -> Any:
            fwk_cache: AsyncRedisCache = AsyncRedisCache(alias, ttl, namespace)
            key = _key_from_args(func, args, kwargs)
            value: SerializableData = await fwk_cache.get(key, namespace=namespace)

            if not value:
                value: SerializableData = await func(*args, **kwargs)
                _: bool = await fwk_cache.put(key, value, namespace=namespace)
            _: await fwk_cache.close()
            return value

        @wraps(func)
        def handled_sync_func(*args, **kwargs) -> Any:
            fwk_cache: SyncRedisCache = SyncRedisCache(alias, ttl, namespace)
            key = _key_from_args(func, args, kwargs)
            value: SerializableData = fwk_cache.get(key, namespace=namespace)

            if not value:
                value: SerializableData = func(*args, **kwargs)
                _: bool = fwk_cache.put(key, value, namespace=namespace)
            _: fwk_cache.close()
            return value

        if asyncio.iscoroutinefunction(func):
            return handled_async_func
        return handled_sync_func
    return manage_params


def memory_cache(alias: str, strategy: Optional[str] = None, max_size: Optional[int] = None,
                 ttl: Optional[int] = None, namespace: Optional[str] = None,
                 getsizeof: Optional[int] = None) -> Any:

    def manage_params(func: Any) -> Any:
        @wraps(func)
        async def handled_async_func(*args, **kwargs) -> Any:
            fwk_cache: AsyncMemoryCache = AsyncMemoryCache(alias, strategy, max_size, ttl,
                                                           namespace, getsizeof)
            key = _key_from_args(func, args, kwargs)
            value: SerializableData = await fwk_cache.get(key, namespace=namespace)

            if not value:
                value: SerializableData = await func(*args, **kwargs)
                _: bool = await fwk_cache.put(key, value, namespace=namespace)
            return value

        @wraps(func)
        def handled_sync_func(*args, **kwargs) -> Any:
            fwk_cache: SyncMemoryCache = SyncMemoryCache(alias, strategy, max_size, ttl,
                                                         namespace, getsizeof)
            key = _key_from_args(func, args, kwargs)
            value: SerializableData = fwk_cache.get(key, namespace=namespace)

            if not value:
                value: SerializableData = func(*args, **kwargs)
                _: bool = fwk_cache.put(key, value, namespace=namespace)
            return value

        if asyncio.iscoroutinefunction(func):
            return handled_async_func
        return handled_sync_func
    return manage_params
