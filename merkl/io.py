import os
import uuid
import shutil
import collections
from pathlib import Path
from functools import partial
from datetime import datetime
from tempfile import mkdtemp

import merkl
from merkl.utils import get_hash_memory_optimized
from merkl.exceptions import SerializationError


cwd = ''


def _get_file_content(path, flags):
    with open(path, 'r'+flags) as f:
        return f.read()


def _get_tmp_filename(suffix='', dir='/tmp/'):
    path = None
    while path is None or os.path.exists(path):
        path = str(Path(dir) / f'{uuid.uuid4().hex}{suffix}')
    return path


def path_future(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    md5_hash = fetch_or_compute_md5(path, store=True)

    def _return_path():
        return path

    return merkl.future.Future(_return_path, hash=md5_hash, meta=path, is_input=True)


def read_future(path, flags=''):
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    md5_hash = fetch_or_compute_md5(path, store=True)
    f = partial(_get_file_content, path=path, flags=flags)
    return merkl.future.Future(f, hash=md5_hash, meta=path, is_input=True)


def write_future(future, path):
    future.output_files.append(path)
    return future


def fetch_or_compute_md5(path, cache=merkl.cache.SqliteCache, store=True):
    modified = os.stat(path).st_mtime
    md5_hash, _ = cache.get_file_mod_hash(path, modified)
    if md5_hash is None:
        md5_hash = get_hash_memory_optimized(path, mode='md5')
        if store:
            cache.add_file(path, modified, md5_hash=md5_hash)

    return md5_hash


def write_track_file(path, content_bytes, merkl_hash, cache=merkl.cache.SqliteCache):
    if isinstance(content_bytes, FileRef):
        shutil.copy(content_bytes, path)
    elif isinstance(content_bytes, DirRef):
        shutil.copytree(content_bytes, path)
    else:
        with open(path, 'wb') as f:
            f.write(content_bytes)

    # NOTE: we could get the md5 hash and store it, but not strictly necessary for
    # files that merkl has "created". The md5 is necessary though for files _read_ by merkl but produced elsewhere
    cache.add_file(path, merkl_hash=merkl_hash)


class FileRef(str):
    def __new__(cls, path=None, ext='bin', rm_after_caching=False):
        if path is None:
            suffix = None if ext is None else f'.{ext}'
            path = _get_tmp_filename(suffix=suffix)

        return str.__new__(cls, path)

    def __init__(self, path=None, ext='.bin', rm_after_caching=False):
        if path is None:
            self.rm_after_caching = True  # always remove temporary files
        else:
            self.rm_after_caching = rm_after_caching

    def __repr__(self):
        return f'<FileRef {self}>'

    def remove(self):
        os.remove(self)


class DirRef(str):
    def __new__(cls, path=None, rm_after_caching=False, files=None):
        if path is None:
            path = mkdtemp()
            os.makedirs(path, exist_ok=True)

        return str.__new__(cls, path)


    def __init__(self, path=None, rm_after_caching=False, files=None):
        if path is None:
            self.rm_after_caching = True  # always remove temporary files
        else:
            self.rm_after_caching = rm_after_caching

        self._files = [] if files is None else files

    def get_new_file(self, name=None, ext=None):
        if name is not None:
            return str(Path(self) / name)

        suffix = None if ext is None else f'.{ext}'
        file_path = _get_tmp_filename(suffix=suffix, dir=self)
        self._files.append(Path(file_path).name)
        return file_path

    @property
    def files(self):
        return [str(Path(self) / name) for name in self._files]

    def load_files(self):
        """ Load filenames from the file system """
        self._files = os.listdir(self)

    def __repr__(self):
        return f'<DirRef {self}>'

    def remove(self):
        shutil.rmtree(self)
