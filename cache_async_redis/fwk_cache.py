from abc import ABC, abstractmethod
from typing import Union, Dict, Any, Optional

from cache.exceptions import ConfigurationError

BackendConfig = Dict[str, Dict[str, Any]]


class Backend:
    REDIS: str = "redis"
    MEMORY: str = "memory"


class Strategy:
    FIFO: str = "cachetools.FIFOCache"
    LFU: str = "cachetools.LFUCache"
    MRU: str = "cachetools.MRUCache"
    RR: str = "cachetools.RRCache"
    TTL: str = "cachetools.TTLCache"
    TLRU: str = "cachetools.TLRUCache"


SerializableData = Union[None, int, float, complex, bool, str, bytes, bytearray, tuple,
                         dict, set, object]
InmutableKey = Union[str, int, float, bool, tuple]
CacheConfig = Dict[str, Dict[str, Any]]


class FwkCache(ABC):

    _config: CacheConfig = {}

    def __str__(self) -> str:
        text: str = ""
        try:
            for key, value in self.__dict__.items():
                text += str(key) + ":" + str(value) + " "
            return text
        except (KeyError, ValueError) as exc:
            return repr(exc)

    @staticmethod
    def load_config(config_: CacheConfig) -> CacheConfig:
        FwkCache._config.update(config_)
        return FwkCache._config

    @staticmethod
    def get_config() -> CacheConfig:
        return FwkCache._config

    @staticmethod
    def _get_alias_config(alias: str) -> Dict[str, Any]:
        try:
            return FwkCache._config[alias]
        except KeyError:
            raise ConfigurationError(f"The alias {alias} not exists as a key in "
                                     f"the configuration {FwkCache._config}")

    @staticmethod
    @abstractmethod
    def _check_validate_backend(alias: str, config: Dict[str, Any]) -> bool:
        """ Check backend """
        pass


class SyncFwkCache(ABC):

    @abstractmethod
    def get(self, key: InmutableKey, default: Optional[object] = None,
            namespace: Optional[str] = None) -> SerializableData:
        """ get """
        pass

    @abstractmethod
    def put(self, key: InmutableKey, data: SerializableData,
            namespace: Optional[str] = None) -> bool:
        """ put """
        pass

    @abstractmethod
    def clear(self, namespace: Optional[str] = None) -> bool:
        """ clear """
        pass

    @abstractmethod
    def delete(self, key: InmutableKey,  namespace: Optional[str] = None) -> int:
        """ delete """
        pass

    @abstractmethod
    def exists(self, key: InmutableKey,  namespace: Optional[str] = None) -> bool:
        """ exists """
        pass


class AsyncFwkCache(ABC):

    @abstractmethod
    async def get(self, key: InmutableKey, default: Optional[object] = None,
                  namespace: Optional[str] = None) -> SerializableData:
        """ get """
        pass

    @abstractmethod
    async def put(self, key: InmutableKey, data: SerializableData,
                  namespace: Optional[str] = None) -> bool:
        """ put """
        pass

    @abstractmethod
    async def clear(self, namespace: Optional[str] = None) -> bool:
        """ clear """
        pass

    @abstractmethod
    async def delete(self, key: InmutableKey,  namespace: Optional[str] = None) -> int:
        """ delete """
        pass

    @abstractmethod
    async def exists(self, key: InmutableKey,  namespace: Optional[str] = None) -> bool:
        """ exists """
        pass
