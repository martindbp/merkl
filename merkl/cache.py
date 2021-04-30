import os
import sqlite3
from typing import NamedTuple


def get_merkl_path():
    from merkl.io import cwd
    return f'{cwd}.merkl/'


def get_db_path():
    return f'{get_merkl_path()}cache.sqlite3'


def get_cache_dir_path(hash=None):
    base = f'{get_merkl_path()}cache/'
    if hash is not None:
        return f'{base}{hash[:2]}/'
    return base


def get_cache_file_path(hash, ext='bin', makedirs=False):
    cache_dir = get_cache_dir_path(hash)
    if makedirs:
        os.makedirs(cache_dir, exist_ok=True)
    return f'{cache_dir}{hash}.{ext}'


def get_modified_time(path):
    try:
        return os.stat(path).st_mtime
    except:
        return None


class SqliteCache:
    connection = None

    @classmethod
    def connect(cls):
        if cls.connection is None:
            cls.connection = sqlite3.connect(get_db_path())
            cls.cursor = cls.connection.cursor()

    @classmethod
    def create_cache(cls):
        if os.path.exists(get_db_path()):
            return

        os.makedirs(get_merkl_path(), exist_ok=True)
        os.makedirs(get_cache_dir_path(), exist_ok=True)
        cls.connect()
        cls.cursor.execute("""
            CREATE TABLE cache (
                hash CHARACTER(64) PRIMARY KEY,
                data BLOB
            )
        """)

        cls.cursor.execute("""
            CREATE TABLE files (
                path TEXT,
                modified INTEGER,
                merkl_hash CHARACTER(64),
                md5_hash CHARACTER(64),
                PRIMARY KEY (path, modified)
            )
        """)

    @classmethod
    def add(cls, hash, content_bytes=None):
        from merkl.io import FileOut
        if isinstance(content_bytes, FileOut):
            file_out = content_bytes
            ext = file_out.path.split('.')[-1]
            cache_file_path = get_cache_file_path(hash, ext, makedirs=True)
            os.link(file_out.path, cache_file_path)
            orig_path = file_out.path
            file_out.path = cache_file_path
            content_bytes = f'<FileOut {file_out.path}>'.encode()
            if file_out.rm_after_caching:
                os.remove(orig_path)


        cls.connect()
        cls.cursor.execute("INSERT INTO cache VALUES (?, ?)", (hash, content_bytes))
        cls.connection.commit()

    @classmethod
    def add_file(cls, path, modified=None, merkl_hash=None, md5_hash=None):
        modified = modified or get_modified_time(path)
        cls.connect()
        cls.cursor.execute("INSERT INTO files VALUES (?, ?, ?, ?)", (path, modified, merkl_hash, md5_hash))
        cls.connection.commit()

    @classmethod
    def get_file_mod_hash(cls, path, modified=None):
        modified = modified or get_modified_time(path)
        cls.connect()
        result = cls.cursor.execute("SELECT md5_hash, merkl_hash FROM files WHERE path=? AND modified=?", (path, modified))
        result = list(result)
        if len(result) == 0:
            return None, None

        assert len(result) == 1
        return result[0]

    @classmethod
    def get_latest_file(cls, path):
        cls.connect()
        result = cls.cursor.execute("""
            SELECT md5_hash, merkl_hash, modified
            FROM files
            WHERE path=? ORDER BY modified DESC
        """, (path,))
        result = list(result)
        if len(result) == 0:
            return None, None, None
        return result[0]

    @classmethod
    def get(cls, hash):
        cls.connect()
        result = cls.cursor.execute("SELECT data FROM cache WHERE hash=?", (hash,))
        result = list(result)
        if len(result) == 0:
            return None

        data = result[0][0]
        if data is None:
            return open(get_cache_file_path(hash), 'rb').read()

        return data

    @classmethod
    def has(cls, hash):
        cls.connect()
        result = cls.cursor.execute("SELECT COUNT(*) FROM cache WHERE hash=?", (hash,))
        result = list(result)
        return result[0][0] > 0

    @classmethod
    def has_file(cls, hash):
        cls.connect()
        result = cls.cursor.execute("SELECT COUNT(*) FROM files WHERE md5_hash=?", (hash,))
        result = list(result)
        return result[0][0] > 0
