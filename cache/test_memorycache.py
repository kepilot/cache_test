from datetime import timedelta, datetime
from typing import Dict, Any, Optional, List, Union, Iterator, Callable

from cache.DElete_cache_configuration import InmutableKey, SerializableData
from cachetools import TTLCache, FIFOCache, LFUCache, MRUCache, RRCache, TLRUCache, Cache
from cache.exceptions import WrongBackendImplementation
from cache.test_cache import AbstractSyncTestCache, TestCache, Strategy, Backend, AbstractAsyncTestCache

CacheConfig = Dict[str, Dict[str, Any]]
MemoryCache = Union[TTLCache, FIFOCache, LFUCache, MRUCache, RRCache, TLRUCache]
MemoryRegions = Dict[str, MemoryCache]

memory_config = {
    'memory': {
        'backend': 'memory',
        'max_size': 100,
        'namespace': 'main',
        'strategy': Strategy.FIFO

    },

    'memory_ttl': {
        'backend': 'memory',
        'namespace': 'test',
        'ttl': 10,
        'strategy': Strategy.TTL
    }
}

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
TestCache.load_cache(memory_config)
TestCache.load_cache(redis_config)


class MemoryCommonCache(TestCache):
    _caches: MemoryRegions = {}

    def __init__(self, alias: str) -> None:
        self._cache: MemoryCache
        self._selected_config: Dict[str, Any] = TestCache.get_alias_config(alias)
        self._alias = alias
        self._selected_config = self.get_alias_config(alias)
        self._backend = self._selected_config.get("backend", Backend.MEMORY)
        self._strategy = self._selected_config.get("strategy", Strategy.TTL)
        self._max_size = self._selected_config.get("max_size", 1_000)
        self._ttl = self._selected_config.get("ttl", None)
        self._namespace = self._selected_config.get("namespace", "")
        self._getsizeof = self._selected_config.get("getsizeof")
        self._check_validate_backend(alias, TestCache._config)
        self._cache: MemoryCache = self._set_cache(alias)

    @property
    def selected_config(self) -> Dict[str, Any]:
        return self._selected_config

    @staticmethod
    def load_cache(config_: CacheConfig) -> CacheConfig:
        MemoryCommonCache._config = config_
        return config_

    @staticmethod
    def get_config() -> CacheConfig:
        return MemoryCommonCache._config

    def get_options(self) -> Dict[str, Any]:
        return {'alias': self._alias,
                'backend': self._backend,
                'strategy': self._strategy,
                'max_size': self._max_size,
                'ttl': self._ttl,
                'namespace': self._namespace,
                'getsizeof': self._getsizeof
                }

    def _nskey(self, key: InmutableKey, namespace: Optional[str] = None) -> str:
        if namespace is not None:
            return namespace + str(key)
        else:
            return self._namespace + str(key)

    def _check_validate_backend(self, alias: str, _config: CacheConfig) -> bool:
        if self._backend != Backend.MEMORY:
            raise WrongBackendImplementation(f"Selectd backend '{self._backend}' not work properly "
                                             f"with the selected implementation "
                                             f"'{self.__class__.__name__}'")
        return True

    def _set_cache(self, alias: str) -> MemoryCache:
        cache: MemoryCache
        self._alias = alias
        self._selected_config = self.get_alias_config(alias)
        self._backend = self._selected_config.get("backend", Backend.MEMORY)
        if self._strategy is None:
            self._strategy = self._selected_config.get("strategy", Strategy.TTL)
        if self._max_size is None:
            self._max_size = self._selected_config.get("max_size", 1_000)
        if self._ttl is None:
            self._ttl = self._selected_config.get("ttl", 3600)
        if self._namespace is None:
            self._namespace = self._selected_config.get("namespace", "")
        if self._getsizeof is None:
            self._getsizeof = self._selected_config.get("getsizeof")

        self._cache_options = f"{self._strategy}_{self._max_size}_{self._ttl}_{self._getsizeof}"
        try:
            cache = MemoryCommonCache._caches[alias]
            if not isinstance(cache, Cache):
                raise KeyError("Is not cachetools instance")

        except KeyError:
            if self._strategy == Strategy.FIFO:
                cache = FIFOCache(self._max_size, self._getsizeof)
            elif self._strategy == Strategy.LFU:
                cache = LFUCache(self._max_size, self._getsizeof)
            elif self._strategy == Strategy.MRU:
                cache = MRUCache(self._max_size, self._getsizeof)
            elif self._strategy == Strategy.RR:
                cache = RRCache(self._max_size, self._getsizeof)
            elif self._strategy == Strategy.TTL:
                cache = TTLCache(self._max_size, self._ttl, getsizeof=self._getsizeof)
            elif self._strategy == Strategy.TLRU:
                def my_ttu(key, value, now):
                    return now + timedelta(seconds=self._ttl)
                cache = TLRUCache(self._max_size, my_ttu, timer=datetime.now,
                                  getsizeof=self._getsizeof)
            else:
                raise NotImplementedError(f"Strategy {self._strategy} is not implemented")

        MemoryCommonCache._caches[alias] = cache
        return cache


class AsyncMemoryCache(MemoryCommonCache, AbstractAsyncTestCache):

    def __init__(self, alias: str) -> None:
        super().__init__(alias)

    async def get(self, key: InmutableKey, default: Optional[object] = None) -> SerializableData:
        value: SerializableData = self._search_key(key)
        if value is not None:
            return value
        return default

    async def put(self, key: InmutableKey, data: SerializableData) -> bool:
        ns_key: str = self._nskey(key, self._namespace)
        self._cache[ns_key] = data
        return True

    async def clear(self) -> bool:
        iter_cache: Iterator = iter(self._cache)
        delete_keys: List[Any] = []
        while True:
            try:
                value: Any = next(iter_cache)
                delete_keys.append(self._cache[value])
            except StopIteration:
                break
        _ = [self._cache.pop(nskey, None) for nskey in delete_keys]
        return True

    async def delete(self, key: InmutableKey) -> int:
        try:
            _: SerializableData = self._search_key(key, pop=True)
            return 1
        except KeyError:
            return 0

    async def exists(self, key: InmutableKey) -> bool:
        value: SerializableData = self._search_key(key)
        if value is not None:
            return True
        return False

    async def close(self) -> bool:
        return True

    def _search_key(self, key: InmutableKey, pop: bool = False) -> Any:
        value: SerializableData
        method_cache: Callable[[InmutableKey], Any]
        ns_key: str = self._nskey(key, self._namespace)
        method_cache = self._method_cache(self._cache, pop)
        value = method_cache(ns_key)
        return value

    @staticmethod
    def _method_cache(cache: MemoryCache, pop: bool = False) -> Callable[[InmutableKey], Any]:
        if pop is True:
            return cache.pop
        else:
            return cache.get


class SyncMemoryCache(MemoryCommonCache, AbstractSyncTestCache):

    def __init__(self, alias: str) -> None:
        super().__init__(alias)

    def get(self, key: InmutableKey, default: Optional[object] = None) -> SerializableData:
        value: SerializableData = self._search_key(key)
        if value is not None:
            return value
        return default

    def put(self, key: InmutableKey, data: SerializableData) -> bool:
        ns_key: str = self._nskey(key)
        self._cache[ns_key] = data
        return True

    def clear(self) -> bool:
        iter_cache: Iterator = iter(self._cache)
        delete_keys: List[Any] = []
        while True:
            try:
                value: Any = next(iter_cache)
                delete_keys.append(self._cache[value])
            except StopIteration:
                break
        _ = [self._cache.pop(nskey, None) for nskey in delete_keys]
        return True

    def delete(self, key: InmutableKey) -> int:
        try:
            _: SerializableData = self._search_key(key, pop=True)
            return 1
        except KeyError:
            return 0

    def exists(self, key: InmutableKey) -> bool:
        value: SerializableData = self._search_key(key)
        if value is not None:
            return True
        return False

    def close(self) -> bool:
        return True

    def _search_key(self, key: InmutableKey, pop: bool = False) -> Any:
        value: SerializableData
        method_cache: Callable[[InmutableKey], Any]

        ns_key: str = self._nskey(key, self._namespace)
        method_cache = self._method_cache(self._cache, pop)
        value = method_cache(ns_key)
        return value

    @staticmethod
    def _method_cache(cache: MemoryCache, pop: bool = False) -> Callable[[InmutableKey], Any]:
        if pop is True:
            return cache.pop
        else:
            return cache.get


TestCache.load_cache(memory_config)

