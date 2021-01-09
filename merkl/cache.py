from typing import NamedTuple

IN_MEMORY_CACHE = {}


class ContentHash(NamedTuple):
    hash: str


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
