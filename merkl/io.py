import os
import pathlib
import shutil
from functools import partial
from datetime import datetime
from merkl.utils import get_hash_memory_optimized
from merkl.cache import SqliteCache
from merkl.exceptions import SerializationError


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
    md5_hash, _ = SqliteCache.get_file_mod_hash(path, modified)
    if md5_hash is None:
        md5_hash = get_hash_memory_optimized(path, mode='md5')
        if store:
            SqliteCache.add_file(path, modified, md5_hash=md5_hash)

    return md5_hash


def write_track_file(path, content_bytes, merkl_hash):
    if isinstance(content_bytes, FileOut):
        shutil.copy(content_bytes.path, path)
    else:
        with open(path, 'wb') as f:
            f.write(content_bytes)

    # NOTE: we could get the md5 hash and store it, but not strictly necessary for
    # files that merkl has "created". The md5 is necessary though for files _read_ by merkl but produced elsewhere
    SqliteCache.add_file(path, merkl_hash=merkl_hash)


class FileOut:
    def __init__(self, path, rm_after_caching=False):
        self.path = path
        self.rm_after_caching = rm_after_caching

    def __str__(self):
        return self.path

    def __repr__(self):
        return f'<FileOut {self.path}>'

    def __getstate__(self):
        # Prevent trying to serialize with pickle (without FileOutSerializer)
        raise SerializationError('To return a "FileOut", set the FileOutSerializer as the task serializer')


class FileOutSerializer:
    @classmethod
    def dumps(cls, file_out):
        if not os.path.exists(file_out.path):
            raise ValueError(f'FileOut {file_out.path} does not exist')
        return file_out

    @classmethod
    def loads(cls, data):
        return FileOut(data.decode('utf-8').removeprefix('<FileOut ').removesuffix('>'))
