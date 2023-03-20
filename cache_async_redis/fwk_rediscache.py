
import pickle
from abc import ABC
from typing import Dict, Any, Optional, List, Final

import aiocache
from redis import Redis
import redis.asyncio
from redis.asyncio import ConnectionPool, Connection, BlockingConnectionPool


# from aiocache.base import BaseCache


from cache.DElete_cache_configuration import InmutableKey, SerializableData
from cache.exceptions import WrongBackendImplementation, ConfigurationError
from cache.test_cache import AbstractSyncTestCache, AbstractAsyncTestCache, TestCache, Backend, CacheConfig

AIOCACHE_MEMORY_CACHE: Final[str] = "aiocache.SimpleMemoryCache"
AICOCACHE_REDIS_CACHE: Final[str] = "aiocache.RedisCache"
STRING_SERIALIZER: Final[str] = "aiocache.serializers.SimpleMemoryCache"
PICKLE_SERIALIZER: Final[str] = "aiocache.serializers.PickleSerializer"
JSON_SERIALIZER: Final[str] = "aiocache.serializers.JsonSerializer"
AIOCACHE_REDIS_PARAMS: Final[List[str]] = ["ttl", "cache", "namespace", "endpoint", "password",
                                           "port", "timeout", "db", "pool_min_size",
                                           "pool_max_size", "serializer", "plugins"]
REDIS_PARAMS: Final[List[str]] = ["ttl", "namespace", "endpoint", "password", "port", "timeout",
                                  "db", "pool_min_size", "pool_max_size"]


redis_config = {
    'redis': {
        'backend': 'redis',
        'max_size': 0,
        'namespace': 'main',
        'alias': "redis",
        'ttl': 15,
        'cache': "aiocache.RedisCache",
        'endpoint': "127.0.0.1",
        'port': 6379,
        'pool_max_size': 10,
        'timeout': 10
    }
}
# default definition is mandatory in aioache
aiocache.caches.set_config({
    'default': {
        'backend': 'redis',
        'namespace': 'main',
        'max_size': 0,
        'alias': "default",
        'ttl': 30,
        'cache': AICOCACHE_REDIS_CACHE,
        'endpoint': "127.0.0.1",
        'port': 6379
    }
})


class RedisCommonCache(TestCache, ABC):

    def __init__(self, alias: str, ttl: Optional[int], namespace: Optional[str]) -> None:
        self._alias: str = alias
        self._backend: str = Backend.REDIS
        self._strategy: str = "REDIS"
        self._max_size: int = 0
        self._ttl: Optional[int] = ttl
        self._namespace: Optional[str] = namespace
        self._timeout: int = 5
        self._selected_config: Dict[str, Any] = {}
        self._check_validate_backend(alias, TestCache._config)

    @property
    def selected_config(self) -> Dict[str, Any]:
        return self._selected_config

    def _check_validate_backend(self, alias: str, _config: CacheConfig) -> bool:
        try:
            backend = _config.get(alias, {})['backend']
        except KeyError:
            raise ConfigurationError(f"Backend is not configured for the alias {alias}")
        if backend != Backend.REDIS:
            raise WrongBackendImplementation(f"Selectd backend '{backend}' not work properly "
                                             f"with the selected implementation '{self.__class__.__name__}'")
        return True

    def get_options(self) -> Dict[str, Any]:
        return {'alias': self._alias,
                'backend': self._backend,
                'ttl': str(self._ttl),
                'namespace': self._namespace,
                'timeout': self._timeout,
                'serializer': 'json'
                }

    def _nskey(self, key: InmutableKey, namespace: Optional[str] = None) -> str:
        if namespace is not None:
            return namespace + str(key)
        else:
            return self._namespace + str(key)

    @staticmethod
    def _serialize(value: SerializableData) -> bytes:
        return pickle.dumps(value)

    @staticmethod
    def _deserialize(value: bytes) -> SerializableData:
        if value is None:
            return None
        return pickle.loads(value, encoding="utf-8")


class AsyncRedisCache(RedisCommonCache, AbstractAsyncTestCache):
    _connection_pool: Optional[Any] = None

    def __init__(self, alias: str = "default", ttl: Optional[int] = None,
                 namespace: Optional[str] = None) -> None:
        super().__init__(alias, ttl, namespace)
        self._cache: redis.asyncio.Redis = self._set_cache(alias)

    def _set_cache(self, alias: str) -> redis.asyncio.Redis:
        # if not exists the alias create the cache on the fly based on the current
        # configuration (default)
        self._selected_config = self._get_alias_config(alias)
        self._backend: str = Backend.REDIS
        self._strategy: str = "REDIS"
        self._max_size: int = 0
        if self._ttl is None:
            self._ttl = self.selected_config.get("ttl", None)
        if self._namespace is None:
            self._namespace = self.selected_config.get("namespace", "")

        host: str = self.selected_config.get("host", "127.0.0.1")
        port: int = self.selected_config.get("port", "6379")
        db: int = self.selected_config.get("db", 0)
        username: Optional[str] = self.selected_config.get("username")
        password: Optional[str] = self.selected_config.get("password")
        self._timeout = self.selected_config.get("timeout", 10)
        max_connections: int = self.selected_config.get("max_connections", 50)
        if not AsyncRedisCache._connection_pool:
            pool = BlockingConnectionPool(host=host, port=port, db=db, username=username, password=password,
                                          max_connections=max_connections, socket_timeout=self._timeout)
            AsyncRedisCache._connection_pool = pool
        # cache: AsyncRedis = AsyncRedis(host=endpoint, port=port, db=db, password=password,
        # socket_timeout=self._timeout)
        cache: redis.asyncio.Redis = redis.asyncio.Redis(connection_pool=AsyncRedisCache._connection_pool)
        return cache

    async def close(self) -> bool:
        await self._cache.close()
        return True

    async def get(self, key: InmutableKey, default: Optional[object] = None,
                  namespace: Optional[str] = None) -> SerializableData:
        ns_key: str = self._nskey(key, namespace)
        value: bytes = await self._cache.get(ns_key)
        return RedisCommonCache._deserialize(value)

    async def put(self, key: InmutableKey, data: SerializableData,
                  namespace: Optional[str] = None) -> bool:
        ns_key: str = self._nskey(key, namespace)
        value: bytes = RedisCommonCache._serialize(data)
        await self._cache.set(ns_key, value, ex=self._ttl)
        return True

    async def clear(self, namespace: Optional[str] = None) -> bool:
        await self._cache.flushdb()
        return True

    async def delete(self, key: InmutableKey, namespace: Optional[str] = None) -> int:
        ns_key: str = self._nskey(key, namespace)
        return await self._cache.delete(ns_key)

    async def exists(self, key: InmutableKey, namespace: Optional[str] = None) -> bool:
        value: SerializableData = await self.get(key, namespace)
        if value is None:
            return False
        return True


class SyncRedisCache(RedisCommonCache, AbstractSyncTestCache):

    def __init__(self, alias: str = "default", ttl: Optional[int] = None,
                 namespace: Optional[str] = None) -> None:
        super().__init__(alias, ttl, namespace)
        self._cache: Redis = self._set_cache(alias)

    def _set_cache(self, alias: str) -> Redis:
        # if not exists the alias create the cache on the fly based on the current
        # configuration (default)
        self._selected_config = self._get_alias_config(alias)
        self._backend: str = Backend.REDIS
        self._strategy: str = "REDIS"
        self._max_size: int = 0
        if self._ttl is None:
            self._ttl = self.selected_config.get("ttl", None)
        if self._namespace is None:
            self._namespace = self.selected_config.get("namespace", "")

        endpoint: str = self.selected_config.get("endpoint", "127.0.0.1")
        port: int = self.selected_config.get("port", "127.0.0.1")
        db: int = self.selected_config.get("db", 0)
        password: Optional[str] = self.selected_config.get("password")
        self._timeout = self.selected_config.get("timeout", 5)

        cache = Redis(host=endpoint, port=port, db=db, password=password,
                      socket_timeout=self._timeout)
        return cache

    def close(self) -> bool:
        self._cache.close()
        return True

    def get(self, key: InmutableKey, default: Optional[object] = None,
            namespace: Optional[str] = None) -> SerializableData:
        ns_key: str = self._nskey(key, namespace)
        return RedisCommonCache._deserialize(self._cache.get(ns_key))

    def put(self, key: InmutableKey, data: SerializableData,
            namespace: Optional[str] = None) -> bool:
        ns_key: str = self._nskey(key, namespace)
        self._cache.set(ns_key, RedisCommonCache._serialize(data), ex=self._ttl)
        return True

    def clear(self, namespace: Optional[str] = None) -> bool:
        self._cache.flushdb()
        return True

    def delete(self, key: InmutableKey, namespace: Optional[str] = None) -> int:
        ns_key: str = self._nskey(key, namespace)
        return self._cache.delete(ns_key)

    def exists(self, key: InmutableKey, namespace: Optional[str] = None) -> bool:
        if self.get(key, namespace) is None:
            return False
        return True


TestCache.load_config(redis_config)
