from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Union, Dict, Any, Optional, Final, List
from cache.exceptions import ConfigurationError

MEMORY_PARAMS: Final[List[str]] = ["backend", "strategy", "max_size", "ttl", "namespace", "getsizeof"]
REDIS_PARAMS: Final[List[str]] = ["backend", "ttl", "namespace", "host", "username", "password", "port", "timeout",
                                  "db", "max_connections"]


class Backend:
    REDIS: str = "redis"
    MEMORY: str = "memory"

    @classmethod
    def __contains__(cls, key):
        return key in hasattr(cls, key)


class Strategy:
    FIFO: str = "FIFO"
    LFU: str = "LFU"
    MRU: str = "MRU"
    RR: str = "RR"
    TTL: str = "TTL"
    TLRU: str = "TLRU"


SerializableData = Union[None, int, float, complex, bool, str, bytes, bytearray, tuple,
                         dict, set, object]
InmutableKey = Union[str, int, float, bool, tuple]
CacheConfig = Dict[str, Dict[str, Any]]


class TestCache(ABC):

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
    def load_cache(config_: CacheConfig) -> CacheConfig:
        TestCache._check_config(config_)
        TestCache._config.update(config_)
        return TestCache._config

    @staticmethod
    def _check_config(config_: CacheConfig) -> CacheConfig:
        if not isinstance(config_, Mapping):
            raise TypeError("The configuration must be a dictionary")
        for key, value in config_.items():
            if "backend" not in value.keys():
                raise ConfigurationError(f"The backend {Backend.MEMORY}/{Backend.REDIS} "
                                         f"is mandatory")

        return TestCache._config

    @staticmethod
    def get_backend_type(alias: str) -> str:
        try:
            alias_config: Dict[str, Any] = TestCache._config[alias]
            return alias_config['backend']

        except KeyError:
            raise ConfigurationError(f"The alias {alias} not exists in the configuration "
                                     f"{TestCache._config}")

    @staticmethod
    def get_config() -> CacheConfig:
        return TestCache._config

    @staticmethod
    def get_alias_config(alias: str) -> Dict[str, Any]:
        try:
            return TestCache._config[alias]
        except KeyError:
            raise ConfigurationError(f"The alias {alias} not exists in the configuration "
                                     f"{TestCache._config}")

    @staticmethod
    @abstractmethod
    def _check_validate_backend(alias: str, config: Dict[str, Any]) -> bool:
        """ Check backend """
        pass


class AbstractAsyncTestCache(ABC):

    @abstractmethod
    async def get(self, key: InmutableKey, default: Optional[object] = None) -> SerializableData:
        """ get """
        pass

    @abstractmethod
    async def put(self, key: InmutableKey, data: SerializableData) -> bool:
        """ put """
        pass

    @abstractmethod
    async def clear(self) -> bool:
        """ clear """
        pass

    @abstractmethod
    async def delete(self, key: InmutableKey) -> int:
        """ delete """
        pass

    @abstractmethod
    async def exists(self, key: InmutableKey) -> bool:
        """ exists """
        pass

    @abstractmethod
    async def close(self) -> bool:
        """ close """
        pass


class AbstractSyncTestCache(ABC):

    @abstractmethod
    def get(self, key: InmutableKey, default: Optional[object] = None) -> SerializableData:
        """ get """
        pass

    @abstractmethod
    def put(self, key: InmutableKey, data: SerializableData) -> bool:
        """ put """
        pass

    @abstractmethod
    def clear(self) -> bool:
        """ clear """
        pass

    @abstractmethod
    def delete(self, key: InmutableKey) -> int:
        """ delete """
        pass

    @abstractmethod
    def exists(self, key: InmutableKey) -> bool:
        """ exists """
        pass

    @abstractmethod
    def close(self) -> bool:
        """ close """
        pass
