import os
import yaml
import pathlib
import shutil
from yaml import Loader, Dumper
from functools import partial
from datetime import datetime
from merkl.exceptions import FileNotTrackedError
from merkl.utils import get_hash_memory_optimized
from merkl.cache import cache_file_path, cache_dir_path, DVCFileCache


cwd = ''


def _get_file_content(md5_hash, flags):
    with open(cache_file_path(md5_hash, cwd), 'r'+flags) as f:
        return f.read()


def path_future(path):
    if not os.path.exists(path):
        raise FileNotFoundError

    try:
        md5_hash = get_and_validate_md5_hash(path)
    except FileNotTrackedError:
        md5_hash = get_hash_memory_optimized(path)

    def _return_path():
        return path

    from merkl.future import Future
    return Future(_return_path, caches=[DVCFileCache], hash=md5_hash, meta=path, is_input=True)


def read_future(path, flags=''):
    from merkl.future import Future
    md5_hash = get_and_validate_md5_hash(path)
    f = partial(_get_file_content, md5_hash=md5_hash, flags=flags)
    return Future(f, caches=[DVCFileCache], hash=md5_hash, meta=path, is_input=True)


def write_future(future, path, track=True):
    future.output_files.append((path, track))
    return future


def get_and_validate_md5_hash(path):
    tracked_file = path + '.dvc'
    # TODO: recurse up if folder

    if not os.path.exists(tracked_file):
        raise FileNotTrackedError(f'{tracked_file} does not exist')

    with open(tracked_file) as f:
        data = yaml.load(f, Loader=Loader)
        return data['outs'][0]['md5']


def get_file_modified_date(path):
    path = pathlib.Path(path)
    return datetime.fromtimestamp(path.stat().st_mtime)


def write_track_file(path, content_bytes, content_hash, track=True):
    with open(path, 'wb') as f:
        f.write(content_bytes)

    if track:
        track_file(path, md5_hash=content_hash)


def track_file(file_path, md5_hash=None):
    """
    1. Hash the file
    2. Create a new file `<file>.dvc` containing the file hash and timestamp
    3. Hard link the file to `.dvc/cache`
    """
    md5_hash = get_hash_memory_optimized(file_path, mode='md5') if md5_hash is None else md5_hash

    merkl_file_path = file_path + '.dvc'
    with open(merkl_file_path, 'w') as f:
        yaml.dump({
            'outs': [
                {'md5': md5_hash},
            ]
        }, f)

    if not os.path.exists('{cwd}.dvc'):
        os.makedirs(cache_dir_path(md5_hash, cwd), exist_ok=True)

    try:
        shutil.copy(file_path, cache_file_path(md5_hash, cwd))
    except FileExistsError:
        pass
