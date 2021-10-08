import os
import shutil
import sqlite3
from typing import NamedTuple

import merkl
from merkl.logger import logger, short_hash
from merkl.utils import collect_dag_futures, nested_collect, function_descriptive_name

MEMORY_CACHE = {}

NO_CACHE = False

BLOB_DB_SIZE_LIMIT_BYTES = 100000  # see link further down on page and blob sizes


def get_merkl_path():
    from merkl.io import cwd
    return f'{cwd}.merkl/'


def get_tmp_dir():
    return f'{get_merkl_path()}tmp/'


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


def clear(outs, keep=False, keep_outs=False):
    from merkl.future import Future
    futures = nested_collect(outs, lambda x: isinstance(x, Future))

    dag_futures = set()
    for future in futures:
        collect_dag_futures(future, dag_futures, include_parent_pipelines=True)

    if keep:
        future_hashes = [future.hash for future in dag_futures]
        SqliteCache.clear_all_except(future_hashes)
    elif keep_outs:
        outs_future_hashes = [future.hash for future in futures]
        SqliteCache.clear_all_except(outs_future_hashes)
    else:
        for future in dag_futures:
            future.clear_cache()


class SqliteCache:
    connection = None
    no_commit = False

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
    def commit(cls):
        cls.connection.commit()

    @classmethod
    def create_cache(cls):
        if os.path.exists(get_db_path()):
            return

        logger.debug(f'Creating cache')
        os.makedirs(get_merkl_path(), exist_ok=True)
        os.makedirs(get_tmp_dir(), exist_ok=True)
        os.makedirs(get_cache_dir_path(), exist_ok=True)
        cls.connect()

        # Increase page size for faster BLOB performance:
        # https://www.sqlite.org/intern-v-extern-blob.html#:~:text=A%20database%20page%20size%20of,a%20separate%20file%20are%20faster.
        cls.cursor.execute("PRAGMA page_size = 16384;")

        cls.cursor.execute("""
            CREATE TABLE cache (
                hash CHARACTER(64) PRIMARY KEY,
                data BLOB,
                size INTEGER,
                ref_path TEXT,
                ref_is_dir BOOL,
                module_function TEXT NULL
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
    def transfer_ref(cls, ref, hash):
        """ Transfers a FileRef/DirRef from the original place in the file system to the merkl cache, and returns
        the new ref with the new path """
        logger.debug(f'Transferring {ref}')
        if isinstance(ref, merkl.io.FileRef):
            splits = ref.split('.')
            ext = None if len(splits) == 1 else splits[-1]
            cache_file_path = get_cache_file_path(hash, ext, makedirs=True)
            if os.path.exists(cache_file_path):
                os.remove(cache_file_path)

            if ref.rm_after_caching:
                os.rename(ref, cache_file_path)
            else:
                shutil.copy(ref, cache_file_path)

            new_file_out = merkl.io.FileRef(cache_file_path)
            return new_file_out
        elif isinstance(ref, merkl.io.DirRef):
            cache_dir_path = get_cache_out_dir_path(hash, makedirs=True)
            if ref.rm_after_caching:
                os.rename(ref, cache_dir_path)
            else:
                shutil.copytree(ref, cache_dir_path)

            ref = merkl.io.DirRef(cache_dir_path, files=ref._files)
            return ref

    @classmethod
    def add(cls, hash, content_bytes=None, ref=None, fn_name=None):
        content_len = len(content_bytes) if content_bytes is not None else 0
        if ref is not None:
            content_len = os.stat(ref).st_size

        ref_path = None if ref is None else str(ref)
        ref_is_dir = isinstance(ref, merkl.io.DirRef)

        if ref is None and content_len > BLOB_DB_SIZE_LIMIT_BYTES:
            # Faster to store data in a file
            cache_file_path = get_cache_file_path(hash, makedirs=True)
            with open(cache_file_path, 'wb') as f:
                f.write(content_bytes)
            content_bytes = None

        cls.connect()
        cls.cursor.execute("INSERT INTO cache VALUES (?, ?, ?, ?, ?, ?)", (hash, content_bytes, content_len, ref_path, ref_is_dir, fn_name))
        if not cls.no_commit:
            cls.connection.commit()

    @classmethod
    def clear(cls, hash):
        logger.debug(f'Clearing {short_hash(hash)}')
        cls.connect()
        result = list(cls.cursor.execute("SELECT ref_path, ref_is_dir, data FROM cache WHERE hash=?", (hash,)))
        if len(result) == 0:
            return

        ref_path, ref_is_dir, data = result[0]
        if ref_path is not None:
            if ref_is_dir:
                shutil.rmtree(ref_path)
            else:
                os.remove(ref_path)

        if data is None:
            # Data stored on file, remove it
            os.remove(get_cache_file_path(hash))

        cls.cursor.execute("DELETE FROM cache WHERE hash=?", (hash,))
        if not cls.no_commit:
            cls.connection.commit()

    @classmethod
    def clear_all_except(cls, hashes):
        num_items_in_cache = list(cls.cursor.execute('SELECT COUNT(*) FROM cache'))[0][0]
        num_deleted = num_items_in_cache - len(hashes)
        if num_deleted > 0:
            logger.warning(f'Deleting approximately {num_deleted} items from cache')

        quoted_hashes = [f'"{hash}"' for hash in hashes]
        hashes_with_files = list(
            cls.cursor.execute(f"SELECT hash, ref_path, ref_is_dir FROM cache WHERE hash NOT IN ({','.join(quoted_hashes)}) AND (data IS NULL OR ref_path IS NOT NULL)")
        )
        cls.cursor.execute(f"DELETE FROM cache WHERE hash NOT IN ({','.join(quoted_hashes)})", )

        if len(hashes_with_files) > 0:
            logger.warning(f'Deleting {len(hashes_with_files)} files or directories from cache')

        for hash, ref_path, ref_is_dir in hashes_with_files:
            if ref_path is not None:
                if ref_is_dir:
                    shutil.rmtree(ref_path)
                else:
                    os.remove(ref_path)
            else:
                os.remove(get_cache_file_path(hash))

    @classmethod
    def track_file(cls, path, modified=None, merkl_hash=None, md5_hash=None):
        logger.debug(f'Hashing file {path} modified={modified} merkl_hash={short_hash(merkl_hash)} md5_hash={short_hash(md5_hash)}')
        modified = modified or get_modified_time(path)
        cls.connect()
        cls.cursor.execute("INSERT INTO files VALUES (?, ?, ?, ?)", (path, modified, merkl_hash, md5_hash))
        if not cls.no_commit:
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
            with open(get_cache_file_path(hash), 'rb') as f:
                return f.read()

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

    @classmethod
    def get_stats(cls, module_function=None):
        cls.connect()
        if module_function is not None:
            return list(cls.cursor.execute("SELECT COUNT(*), SUM(size) FROM cache WHERE module_function=?", (module_function,)))[0]
        else:
            return list(cls.cursor.execute("SELECT module_function, COUNT(*), SUM(size) FROM cache GROUP BY module_function"))

    @classmethod
    def clear_module_function(cls, module_function):
        cls.connect()
        result = list(cls.cursor.execute("DELETE FROM cache WHERE module_function=?", (module_function,)))
        return result
