import os
import json
import pathlib
import shutil
from functools import partial
from datetime import datetime
from merkl.future import Future
from merkl.exceptions import FileNotTrackedError, NonSerializableArgError, TrackedFileNotUpToDateError
from merkl.utils import get_hash_memory_optimized
from merkl.cache import cache_file_path, cache_dir_path, FileCache


cwd = ''


class TrackedFilePath:
    """ Class to indicate that path refers to a merkl-tracked file, and hash should be used instead of path string as
    dependency """
    __slots__ = ['path', 'hash']

    def __init__(self, path):
        self.path = path
        merkl_path = path + '.merkl'
        if not os.path.exists(merkl_path):
            raise FileNotTrackedError

        with open(merkl_path, 'r') as f:
            self.hash = json.loads(f.read())['md5_hash']

    @classmethod
    def get_dir_paths(cls, path):
        tracked_paths = []
        for path in pathlib.Path(path).rglob('*.merkl'):
            if os.path.isdir(path):
                continue
            tracked_paths.append(TrackedFilePath(str(path).rstrip('.merkl')))
        return tracked_paths

    def __repr__(self):
        return f'<TrackedFilePath: {self.hash}>'


class FilePath:
    """ Class to indicate that path file whose hash should be used instead of path string as dependency """
    __slots__ = ['path', 'hash']

    def __init__(self, path):
        self.path = path
        self.hash = get_hash_memory_optimized(path, mode='md5')

    @classmethod
    def get_dir_paths(cls, path, pattern='*'):
        paths = []
        for path in pathlib.Path(path).glob(pattern):
            if os.path.isdir(path):
                continue
            paths.append(FilePath(str(path)))
        return paths

    def __repr__(self):
        return f'<Path: {self.hash}>'


def _get_file_object(md5_hash, flags):
    return open(cache_file_path(md5_hash, cwd), 'r'+flags)


def _get_file_content(md5_hash, flags):
    with open(cache_file_path(md5_hash, cwd), 'r'+flags) as f:
        return f.read()


def mread(path, flags=''):
    md5_hash = get_and_validate_md5_hash(path)
    f = partial(_get_file_content, md5_hash=md5_hash, flags=flags)
    return Future(f, caches=[FileCache], hash=md5_hash, meta=path, is_input=True)


def mopen(path, flags=''):
    md5_hash = get_and_validate_md5_hash(path)
    f = partial(_get_file_object, md5_hash=md5_hash, flags=flags)
    return Future(f, caches=[FileCache], hash=md5_hash, meta=path, is_input=True)


def _mwrite_post_eval_hook(future, path, track):
    # Copy file from cache (where it has been serialized) to `path`
    shutil.copy(cache_file_path(future.hash), path)
    if track:
        track_file(path)


def mwrite(future, path, track=True) -> None:
    # To make sure the future gets written to the file cache, simply add FileCache to the future's caches
    if FileCache not in future._caches:
        future._caches.append(FileCache)

    # Create dummy Future with a post eval hook that copies the file from cache, and tracks the file (updates <path>.merkl)
    # Also, we don't use the original future for this, because might want to write the future to multiple files, and we
    # need a separate node for visualization
    hook = partial(_mwrite_post_eval_hook, path=path, track=track)
    return Future(caches=[FileCache], hash=future.hash, meta=path, is_output=True, post_eval_hooks=[hook])


def get_and_validate_md5_hash(path):
    merkl_file = path + '.merkl'
    if not os.path.exists(merkl_file):
        raise FileNotTrackedError

    with open(merkl_file) as f:
        data = json.load(f)
        md5_hash = data['md5_hash']
        timestamp = data['modified_timestamp']
        previous_modified_date = datetime.fromisoformat(timestamp)
        current_modified_date = get_file_modified_date(path)
        if current_modified_date != previous_modified_date:
            raise TrackedFileNotUpToDateError

        return md5_hash


def get_file_modified_date(path):
    path = pathlib.Path(path)
    return datetime.fromtimestamp(path.stat().st_mtime)


def track_file(file_path, gitignore_path='.gitignore'):
    """
    1. Hash the file
    2. Create a new file `<file>.merkl` containing the file hash and timestamp
    3. Hard link the file to `.merkl/cache`
    4. Add `<file>` to `.gitignore` if there is one
    """
    gitignore_exists = os.path.exists('.gitignore')
    md5_hash = get_hash_memory_optimized(file_path, mode='md5')

    merkl_file_path = file_path + '.merkl'
    with open(merkl_file_path, 'w') as f:
        json.dump({
            'md5_hash': md5_hash,
            'modified_timestamp': get_file_modified_date(file_path).isoformat(),
        }, f, sort_keys=True, indent=4)

    if not os.path.exists('{cwd}.merkl'):
        os.makedirs(cache_dir_path(md5_hash, cwd), exist_ok=True)

    try:
        os.link(file_path, cache_file_path(md5_hash, cwd))
    except FileExistsError:
        pass

    if gitignore_exists:
        with open(gitignore_path, 'r') as f:
            lines = set(f.readlines())

        if file_path not in lines:
            with open(gitignore_path, 'a') as f:
                f.write('\n' + file_path)
