import os
from typing import NamedTuple

cache_override = 0  # 0 means no cache override

IN_MEMORY_CACHE = {}

def cache_dir_path(hash, cwd=''):
    return f'{cwd}.merkl/cache/{hash[:2]}'


def cache_file_path(hash, cwd=''):
    return f'{cache_dir_path(hash, cwd)}/{hash}'


def get_cache_from_arg(arg):
    if arg is not None:
        if arg == 'file':
            return FileCache
        else:
            return InMemoryCache


class ContentHash(NamedTuple):
    hash: str


class CacheOverride:
    def __init__(self, cache):
        self.cache = cache

    def __enter__(self, *args, **kwargs):
        global cache_override
        if self.cache is not None:
            cache_override = self.cache

    def __exit__(self, *args, **kwargs):
        global cache_override
        if self.cache is not None:
            cache_override = 0


class InMemoryCache:
    @classmethod
    def add(cls, content_hash, merkle_hash, content_bytes):
        IN_MEMORY_CACHE[merkle_hash] = ContentHash(content_hash)
        IN_MEMORY_CACHE[content_hash] = content_bytes

    @classmethod
    def get(cls, hash, is_content_hash=False):
        value = IN_MEMORY_CACHE.get(hash)
        if isinstance(value, ContentHash):
            return IN_MEMORY_CACHE.get(value.hash)

    @classmethod
    def has(cls, hash, is_content_hash=False):
        return hash in IN_MEMORY_CACHE


class FileCache:
    @classmethod
    def add(cls, content_hash, merkle_hash, content_bytes):
        os.makedirs(cache_dir_path(content_hash), exist_ok=True)
        os.makedirs(cache_dir_path(merkle_hash), exist_ok=True)

        content_path = cache_file_path(content_hash)
        merkle_path = cache_file_path(merkle_hash)
        with open(content_path, 'wb') as f:
            f.write(content_bytes)

        os.link(content_path, merkle_path)

    @classmethod
    def get(cls, hash, is_content_hash=False):
        path = cache_file_path(hash)
        if not os.path.exists(path):
            return None

        with open(path, 'rb') as f:
            return f.read()

    @classmethod
    def has(cls, hash, is_content_hash=False):
        path = cache_file_path(hash)
        return os.path.exists(path)
