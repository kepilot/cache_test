import pickle
from typing import Any, Dict, Optional, Union, List

import redis
import redis.asyncio

from cache.exceptions import WrongBackendImplementation
from cache.test_cache import (
    AbstractAsyncTestCache,
    AbstractSyncTestCache,
    Backend,
    CacheConfig,
    TestCache,
    InmutableKey,
    SerializableData,
)

RedisCache = Union[redis.asyncio.Redis, redis.Redis]
RedisConnectionPool = Union[redis.asyncio.BlockingConnectionPool, redis.BlockingConnectionPool]


redis_config = {
    'redis': {
        'backend': 'redis',
        'namespace': 'main',
        'ttl': 20,
        'host': "127.0.0.1",
        'port': 6379,
        'timeout': 3
    },

    'redis_decorator': {
        'backend': 'redis',
        'namespace': 'main',
        'ttl': 15,
        'host': "127.0.0.1",
        'port': 6379,
        'max_pool_connections': 100,
        'timeout': 10
    }

}


class RedisCommonCache(TestCache):
    _connections_pool: Dict[str, Dict[bool, RedisConnectionPool]] = {}

    def __init__(self, alias: str, asynchronous: bool) -> None:
        self._alias: str = alias
        self._asynchronous: bool = asynchronous
        self._selected_config: Dict[str, Any] = TestCache.get_alias_config(alias)
        self._alias = alias
        self._selected_config = self.get_alias_config(alias)
        self._backend = self._selected_config.get("backend", Backend.MEMORY)
        self._ttl = self._selected_config.get("ttl", None)
        self._namespace = self._selected_config.get("namespace", "")
        self._host: str = self.selected_config.get("host", "127.0.0.1")
        self._port: int = self.selected_config.get("port", "6379")
        self._db: int = self.selected_config.get("db", 0)
        self._max_connections: int = self.selected_config.get(
            "max_pool_connections", 10
        )
        self._username: Optional[str] = self.selected_config.get("username")
        self._password: Optional[str] = self.selected_config.get("password")
        self._timeout = self.selected_config.get("timeout", 10)
        self._check_validate_backend(alias, TestCache._config)
        self._cache: RedisCache = self._set_cache(alias, asynchronous)

    @property
    def selected_config(self) -> Dict[str, Any]:
        return self._selected_config

    def _set_cache(self, alias: str, asynchronous: bool = False) -> RedisCache:
        cache: RedisCache
        pool: RedisConnectionPool
        pool: Optional[RedisConnectionPool] = RedisCommonCache._connections_pool.get(
            alias, {}
        ).get(asynchronous)
        if asynchronous is True:
            if pool is None:
                pool = redis.asyncio.BlockingConnectionPool(
                    host=self._host,
                    port=self._port,
                    db=self._db,
                    username=self._username,
                    password=self._password,
                    socket_timeout=self._timeout,
                    max_connections=self._max_connections,
                )
                RedisCommonCache._add_to_pools(pool, alias, asynchronous)
            cache = redis.asyncio.Redis(connection_pool=pool)
        else:

            if pool is None:
                pool = redis.BlockingConnectionPool(
                    host=self._host,
                    port=self._port,
                    db=self._db,
                    username=self._username,
                    password=self._password,
                    socket_timeout=self._timeout,
                    max_connections=self._max_connections,
                )
                RedisCommonCache._add_to_pools(pool, alias, asynchronous)
            cache = redis.Redis(connection_pool=pool)
        return cache

    @staticmethod
    def _add_to_pools(pool, alias: str, asynchronous: bool) -> None:
        alias_dict: dict = RedisCommonCache._connections_pool.get(alias, {})
        if not alias_dict:
            RedisCommonCache._connections_pool[alias] = {}
        asynchronous_dict: dict = RedisCommonCache._connections_pool[alias].get(
            asynchronous, {}
        )
        if not asynchronous_dict:
            RedisCommonCache._connections_pool[alias][asynchronous] = pool

    def _check_validate_backend(self, alias: str, _config: CacheConfig) -> bool:
        if self._backend != Backend.REDIS:
            raise WrongBackendImplementation(
                f"Selectd backend '{self._backend}' not work properly "
                f"with the selected implementation "
                f"'{self.__class__.__name__}'"
            )
        return True

    def get_options(self) -> Dict[str, Any]:
        return {
            "alias": self._alias,
            "backend": self._backend,
            "ttl": self._ttl,
            "namespace": self._namespace,
            "timeout": self._timeout,
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
    def __init__(self, alias: str) -> None:
        super().__init__(alias, True)

    async def get(
        self, key: InmutableKey, default: Optional[object] = None
    ) -> SerializableData:
        ns_key: str = self._nskey(key)
        value: bytes = await self._cache.get(ns_key)
        return RedisCommonCache._deserialize(value)

    async def put(self, key: InmutableKey, data: SerializableData) -> bool:
        ns_key: str = self._nskey(key)
        value: bytes = RedisCommonCache._serialize(data)
        result: Optional[bool] = await self._cache.set(ns_key, value, ex=self._ttl)
        return result or False

    async def clear(self) -> bool:
        _: Any = await self._cache.flushdb()
        return True

    async def delete(self, key: InmutableKey) -> int:
        ns_key: str = self._nskey(key)
        result: int = await self._cache.delete(ns_key)
        return result

    async def exists(self, key: InmutableKey) -> bool:
        value: SerializableData = await self.get(key)
        if value is None:
            return False
        return True

    async def close(self) -> bool:
        await self._cache.close()
        return True


class SyncRedisCache(RedisCommonCache, AbstractSyncTestCache):
    def __init__(self, alias: str) -> None:
        super().__init__(alias, False)

    def get(
        self, key: InmutableKey, default: Optional[object] = None
    ) -> SerializableData:
        ns_key: str = self._nskey(key)
        value: bytes = self._cache.get(ns_key)
        return RedisCommonCache._deserialize(value)

    def put(self, key: InmutableKey, data: SerializableData) -> bool:
        ns_key: str = self._nskey(key)
        value: bytes = RedisCommonCache._serialize(data)
        self._cache.set(ns_key, value, ex=self._ttl)
        return True

    def clear(self) -> bool:
        _: Any = self._cache.flushdb()
        return True

    def delete(self, key: InmutableKey) -> int:
        ns_key: str = self._nskey(key)
        result: int = self._cache.delete(ns_key)
        return result

    def exists(self, key: InmutableKey) -> bool:
        value: SerializableData = self.get(key)
        if value is None:
            return False
        return True

    def close(self) -> bool:
        self._cache.close()
        return True


TestCache.load_cache(redis_config)

