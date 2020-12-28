IN_MEMORY_CACHE = {}

class InMemoryCache:

    @classmethod
    def put(value, merkle_hash):
        CACHE[merkle_hash] = value

    @classmethod
    def get(merkle_hash):
        return CACHE.get(merkle_hash)
