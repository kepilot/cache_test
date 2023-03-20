from cache.DElete_cache_configuration import SerializableData
from cache.decorators import cached
from utils import get_sync_data, get_async_data


@cached("redis")
def redis_get_data_sync(username: str) -> SerializableData:
    return get_sync_data(username)


@cached("redis_decorator")
async def redis_get_data_async(username: str) -> SerializableData:
    return await get_async_data(username)


@cached("memory_ttl")
def memory_get_data_sync(username: str) -> SerializableData:
    return get_sync_data(username)


@cached("memory_ttl")
async def memory_get_data_async(username: str) -> SerializableData:
    return await get_async_data(username)


def aiocache_localmem_data_sync(username: str) -> SerializableData:
    return get_sync_data(username)


async def aiocache_localmem_data_async(username: str) -> SerializableData:
    return await get_async_data(username)


async def aiocache_redis_data_async(username: str) -> SerializableData:
    return await get_async_data(username)


def aiocache_redis_data_sync(username: str) -> SerializableData:
    return get_sync_data(username)
