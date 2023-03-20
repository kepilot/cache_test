from abc import ABC
from datetime import timedelta, datetime
from typing import Dict, Any, Optional, List, Union, Iterator, Callable

from cache.DElete_cache_configuration import InmutableKey, SerializableData
from cachetools import TTLCache, FIFOCache, LFUCache, MRUCache, RRCache, TLRUCache, Cache
from cache.exceptions import WrongBackendImplementation, ConfigurationError
from cache.test_cache import AbstractSyncTestCache, TestCache, Strategy, Backend, AbstractAsyncTestCache

CacheConfig = Dict[str, Dict[str, Any]]
MemoryCache = Union[TTLCache, FIFOCache, LFUCache, MRUCache, RRCache, TLRUCache]
MemoryRegions = Dict[str, Dict[str, Dict[str, MemoryCache]]]

memory_config = {
    'memory': {
        'backend': 'memory',
        'max_size': 100,
        'namespace': 'main',
        'alias': "memory",
        'ttl': 30,
        'strategy': Strategy.FIFO

    }
}


class MemoryCommonCache(TestCache, ABC):
    _caches: MemoryRegions = {}

    def __init__(self, alias: str, strategy: Optional[str], max_size: Optional[int],
                 ttl: Optional[int], namespace: Optional[str],
                 getsizeof: Optional[int]) -> None:
        self._cache: MemoryCache
        self._alias: str = alias
        self._backend: str = Backend.MEMORY
        self._strategy: Optional[str] = strategy
        self._max_size: Optional[int] = max_size
        self._ttl: Optional[int] = ttl
        self._namespace: Optional[str] = namespace
        self._getsizeof: Optional[int] = getsizeof
        self._selected_config: Dict[str, Any] = {}
        self._cache_options: str = ""
        self._check_validate_backend(alias, MemoryCommonCache._config)
        self._set_cache(alias)

    @property
    def selected_config(self) -> Dict[str, Any]:
        return self._selected_config

    @staticmethod
    def load_config(config_: CacheConfig) -> CacheConfig:
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
        try:
            backend = _config.get(alias, {})['backend']
        except KeyError:
            raise ConfigurationError(f"Backend is not configured for the alias {alias}")

        if backend != Backend.MEMORY:
            raise WrongBackendImplementation(f"Selectd backend '{backend}' not work properly "
                                             f"with the selected implementation "
                                             f"'{self.__class__.__name__}'")
        return True

    def _set_cache(self, alias: str) -> None:
        cache: MemoryCache
        self._alias = alias
        self._selected_config = self._get_alias_config(alias)
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
            cache = MemoryCommonCache._caches[alias][self._namespace][self._cache_options]
            self._cache = cache
            if not isinstance(cache, Cache):
                raise KeyError("Is not cahetools instance")
            return None

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

        self._cache = cache
        self._add_to_caches(self._cache, self._alias, self._namespace, self._cache_options)
        return None

    @staticmethod
    def _add_to_caches(cache, alias: str, namespace: str, cache_options: str) -> None:
        alias_dict: dict = MemoryCommonCache._caches.get(alias, {})
        if not alias_dict:
            MemoryCommonCache._caches[alias] = {}
        namespace_dict: dict = MemoryCommonCache._caches[alias].get(namespace, {})
        if not namespace_dict:
            MemoryCommonCache._caches[alias][namespace] = {}
        cache_options_dict: dict = MemoryCommonCache._caches[alias][namespace].get(cache_options,
                                                                                   {})
        if not cache_options_dict:
            MemoryCommonCache._caches[alias][namespace][cache_options] = cache

    def _get_caches(self, namespace: Optional[str]) -> List[MemoryCache]:
        if namespace is None:
            try:
                namespaces: Dict[str, Any] = MemoryCommonCache._caches[self._alias] or {}
                caches: List[MemoryCache] = []
                for namespace, options in namespaces.items():
                    for cache in list(options.values()):
                        caches.append(cache)
                return caches
            except (KeyError, AttributeError):
                return []
        try:
            value = MemoryCommonCache._caches[self._alias][namespace].values()
            return list(value)
        except (KeyError, AttributeError):
            return []

    @staticmethod
    def _get_alias_config(alias: str) -> Dict[str, Any]:
        try:
            return MemoryCommonCache._config[alias]
        except KeyError:
            raise ConfigurationError(f"The configuration with alias {alias} not exists")


class AsyncMemoryCache(MemoryCommonCache, AbstractAsyncTestCache):

    def __init__(self, alias: str, strategy: Optional[str] = None, max_size: Optional[int] = None,
                 ttl: Optional[int] = None, namespace: Optional[str] = None,
                 getsizeof: Optional[int] = None) -> None:
        super().__init__(alias, strategy, max_size, ttl, namespace, getsizeof)

    async def get(self, key: InmutableKey, default: Optional[object] = None,
                  namespace: Optional[str] = None) -> SerializableData:
        value: SerializableData = self._search_key(key, namespace)
        if value is not None:
            return value
        return default

    async def put(self, key: InmutableKey, data: SerializableData,
                  namespace: Optional[str] = None) -> bool:
        ns_key: str = self._nskey(key, namespace)
        self._cache[ns_key] = data
        return True

    async def clear(self, namespace: Optional[str] = None) -> bool:
        for cache in self._get_caches(namespace):
            iter_cache: Iterator = iter(cache)
            delete_keys: List[Any] = []
            while True:
                try:
                    value: Any = next(iter_cache)
                    delete_keys.append(cache[value])
                except StopIteration:
                    break
            _ = [cache.pop(key, None) for key in delete_keys]
        return True

    async def delete(self, key: InmutableKey,  namespace: Optional[str] = None) -> int:
        try:
            _: SerializableData = self._search_key(key, namespace, pop=True)
            return 1
        except KeyError:
            return 0

    async def exists(self, key: InmutableKey,  namespace: Optional[str] = None) -> bool:
        value: SerializableData = self._search_key(key, namespace)
        if value is not None:
            return True
        return False

    def _search_key(self, key: InmutableKey,  namespace: Optional[str] = None,
                    pop: bool = False) -> Any:
        value: SerializableData
        method_cache: Callable[[InmutableKey], Any]

        ns_key: str = self._nskey(key, namespace)
        method_cache = self._method_cache(self._cache, pop)
        value = method_cache(ns_key)
        if value is not None:
            return value
        else:
            for cache in self._get_caches(namespace):
                method_cache = self._method_cache(cache, pop)
                value: SerializableData = method_cache(ns_key)
                if value is not None:
                    return value
            return None

    @staticmethod
    def _method_cache(cache: MemoryCache, pop: bool = False) -> Callable[[InmutableKey], Any]:
        if pop is True:
            return cache.pop
        else:
            return cache.get


class SyncMemoryCache(MemoryCommonCache, AbstractSyncTestCache):

    def __init__(self, alias: str, strategy: Optional[str] = None, max_size: Optional[int] = None,
                 ttl: Optional[int] = None, namespace: Optional[str] = None,
                 getsizeof: Optional[int] = None) -> None:
        super().__init__(alias, strategy, max_size, ttl, namespace, getsizeof)

    def get(self, key: InmutableKey, default: Optional[object] = None,
            namespace: Optional[str] = None) -> SerializableData:
        value: SerializableData = self._search_key(key, namespace)
        if value is not None:
            return value
        return default

    def put(self, key: InmutableKey, data: SerializableData, namespace: Optional[str] = None) -> bool:
        ns_key: str = self._nskey(key, namespace)
        self._cache[ns_key] = data
        return True

    def clear(self, namespace: Optional[str] = None) -> bool:
        for cache in self._get_caches(namespace):
            iter_cache: Iterator = iter(cache)
            delete_keys: List[Any] = []
            while True:
                try:
                    value: Any = next(iter_cache)
                    delete_keys.append(cache[value])
                except StopIteration:
                    break
            _ = [cache.pop(key, None) for key in delete_keys]
        return True

    def delete(self, key: InmutableKey,  namespace: Optional[str] = None) -> int:
        try:
            _: SerializableData = self._search_key(key, namespace, pop=True)
            return 1
        except KeyError:
            return 0

    def exists(self, key: InmutableKey,  namespace: Optional[str] = None) -> bool:
        value: SerializableData = self._search_key(key, namespace)
        if value is not None:
            return True
        return False

    def _search_key(self, key: InmutableKey, namespace: Optional[str] = None,
                    pop: bool = False) -> Any:
        value: SerializableData
        method_cache: Callable[[InmutableKey], Any]
        ns_key: str = self._nskey(key, namespace)
        method_cache = self._method_cache(self._cache, pop)
        value = method_cache(ns_key)
        if value is not None:
            return value
        else:
            for cache in self._get_caches(namespace):
                method_cache = self._method_cache(cache, pop)
                value: SerializableData = method_cache(ns_key)
                if value is not None:
                    return value
            return None

    @staticmethod
    def _method_cache(cache: MemoryCache, pop: bool = False) -> Callable[[InmutableKey], Any]:
        if pop is True:
            return cache.pop
        else:
            return cache.get


TestCache.load_config(memory_config)

