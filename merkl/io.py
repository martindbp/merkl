import os
import pathlib
import shutil
from functools import partial
from datetime import datetime
from merkl.utils import get_hash_memory_optimized
from merkl.cache import SqliteCache


cwd = ''


def _get_file_content(path, flags):
    with open(path, 'r'+flags) as f:
        return f.read()


def path_future(path):
    if not os.path.exists(path):
        raise FileNotFoundError

    md5_hash = fetch_or_compute_md5(path, store=True)

    def _return_path():
        return path

    from merkl.future import Future
    return Future(_return_path, hash=md5_hash, meta=path, is_input=True)


def read_future(path, flags=''):
    if not os.path.exists(path):
        raise FileNotFoundError

    from merkl.future import Future
    md5_hash = fetch_or_compute_md5(path, store=True)
    f = partial(_get_file_content, path=path, flags=flags)
    return Future(f, hash=md5_hash, meta=path, is_input=True)


def write_future(future, path):
    future.output_files.append(path)
    return future


def fetch_or_compute_md5(path, store=True):
    modified = os.stat(path).st_mtime
    md5_hash, _ = SqliteCache.get_file_hash(path, modified)
    if md5_hash is None:
        md5_hash = get_hash_memory_optimized(path, mode='md5')
        if store:
            SqliteCache.add_file(path, modified, md5_hash=md5_hash)

    return md5_hash


def write_track_file(path, content_bytes, merkl_hash):
    with open(path, 'wb') as f:
        f.write(content_bytes)

    modified = os.stat(path).st_mtime
    # NOTE: we could get the md5 hash and store it, but not strictly necessary for
    # files that merkl has "created". The md5 is necessary though for files _read_ by merkl but produced elsewhere
    SqliteCache.add_file(path, modified, merkl_hash=merkl_hash)
