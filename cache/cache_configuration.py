from functools import wraps
from typing import Union, Dict, Any, Final

import cachetools
from cache.fwk_cache import Strategy
from aiocache import caches
SerializableData = Union[None, int, float, complex, bool, str, bytes, bytearray, tuple,
                         dict, set, object]
InmutableKey = Union[str, int, float, bool, tuple]


# default definition in aioache is mandatory
# caches.set_config({'default': {'cache': "aiocache.SimpleMemoryCache",
#                                'ttl': 90,
#                                'serializer': {'class': "aiocache.serializers.StringSerializer"}
#                                         }
#                            })
#
LOCAL_MEMORY: Final[Dict[str, Any]] = {'cache': "aiocache.SimpleMemoryCache",
                                       'ttl': 40,
                                       'serializer': {'class': "aiocache.serializers.PickleSerializer"}
                                       }
REDIS: Final[Dict[str, Any]] = {'cache': "aiocache.RedisCache",
                                'ttl': 40,
                                'endpoint': "127.0.0.1", 'port': 6379, 'timeout': 3,
                                'serializer': {'class': "aiocache.serializers.PickleSerializer"},
                                'plugins': []
                                }


# caches.set_config
redis_config = {
    'default': {
        'ttl': 15,
        'backend': 'memory',
        'max_size': 0,
        'alias': "default",
        'cache': "aiocache.SimpleMemoryCache",
        'serializer': {
            'class': "aiocache.serializers.StringSerializer"
        }
    },
    'local_memory': {
        'ttl': 4,
        'cache': "aiocache.SimpleMemoryCache",
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
        }
    },
    'local_mem': {
        'ttl': 10,
        'cache': "aiocache.SimpleMemoryCache",
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
        }
    },
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
        'timeout': 10,
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
            },
        'plugins': []
    },
    'redis2': {
        'backend': 'redis',
        'max_size': 0,
        'namespace': 'main',
        'alias': "redis",
        'ttl': 5,
        'cache': "aiocache.RedisCache",
        'endpoint': "127.0.0.1",
        'port': 6379,
        'timeout': 3,
        'serializer': {
            'class': "aiocache.serializers.PickleSerializer"
        },
    }
}

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


class _HashedSeq(list):
    """ This class guarantees that hash() will be called no more than once
        per element.  This is important because the lru_cache() will hash
        the key multiple times on a cache miss.
    """

    __slots__ = 'hashvalue'

    def __init__(self, tup):
        self[:] = tup
        self.hashvalue = hash(tup)

    def __hash__(self):
        return self.hashvalue


def _key_from_args(func, args, kwargs):
    ordered_kwargs = frozenset(sorted(kwargs.items()))
    return (func.__module__ or '') + func.__name__ + str(args) + str(ordered_kwargs)


def _make_key(args, kwds, typed, kwd_mark=(object(),)):
    """Make a cache key from optionally typed positional and keyword arguments
    The key is constructed in a way that is flat as possible rather than
    as a nested structure that would take more memory.
    If there is only a single argument and its data type is known to cache
    its hash value, then that argument is returned without a wrapper.  This
    saves space and improves lookup speed.
    """
    # All of code below relies on kwds preserving the order input by the user.
    # Formerly, we sorted() the kwds before looping.  The new way is *much*
    # faster; however, it means that f(x=1, y=2) will now be treated as a
    # distinct call from f(y=2, x=1) which will be cached separately.
    key = args
    if kwds:
        key += kwd_mark
        for item in kwds.items():
            key += item
    if typed:
        key += tuple(type(v) for v in args)
        if kwds:
            key += tuple(type(v) for v in kwds.values())
    return _HashedSeq(key)



