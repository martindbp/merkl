import os
import shutil
import pickle
import sqlite3
from typing import NamedTuple

NO_CACHE = False

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


def get_cache_out_dir_path(hash, makedirs=False):
    cache_dir = get_cache_dir_path(hash)
    if makedirs:
        os.makedirs(cache_dir, exist_ok=True)
    cache_dir = f'{cache_dir}{hash}/'
    return cache_dir


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
            try:
                cls.connection = sqlite3.connect(get_db_path())
            except sqlite3.OperationalError:
                if not os.path.exists(get_merkl_path()):
                    print(".merkl doesn't exist, did you run 'merkl init'?")
                    exit(1)
                raise
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
    def transfer_file_out(cls, file_out, hash):
        """ Transfers a FileRef from the original place in the file system to the merkl cache, and returns
        the new FileRef with the new path """
        from merkl.io import FileRef
        splits = file_out.split('.')
        ext = None if len(splits) == 1 else splits[-1]
        cache_file_path = get_cache_file_path(hash, ext, makedirs=True)
        if os.path.exists(cache_file_path):
            os.remove(cache_file_path)

        os.link(file_out, cache_file_path)
        new_file_out = FileRef(cache_file_path)
        if file_out.rm_after_caching:
            os.remove(file_out)
        return new_file_out

    @classmethod
    def transfer_dir_out(cls, dir_out, hash):
        """ Transfers a DirRef from the original place in the file system to the merkl cache, and returns
        the new DirRef with the new path """
        from merkl.io import DirRef
        cache_dir_path = get_cache_out_dir_path(hash, makedirs=True)
        shutil.copytree(dir_out, cache_dir_path)
        if dir_out.rm_after_caching:
            shutil.rmtree(dir_out)

        dir_out = DirRef(cache_dir_path, files=dir_out._files)
        return dir_out

    @classmethod
    def add(cls, hash, content_bytes=None):
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
