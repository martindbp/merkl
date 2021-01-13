import os
import pathlib
import json
from datetime import datetime
from merkl.future import Future
from merkl.exceptions import FileNotTrackedError, NonSerializableArgError, TrackedFileNotUpToDateError
from merkl.utils import get_hash_memory_optimized
from merkl.cache import cache_file_path, cache_dir_path


class TrackedPath:
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
            tracked_paths.append(TrackedPath(str(path).rstrip('.merkl')))
        return tracked_paths

    def __repr__(self):
        return f'TrackedPath: {self.hash}'


class FileObjectFuture(Future):
    def __init__(self, path, flags, cwd=''):
        md5_hash = get_and_validate_md5_hash(path)

        def _get_file_object():
            return open(cache_file_path(md5_hash, cwd), flags)

        super().__init__(_get_file_object, '', hash=md5_hash)


class FileContentFuture(Future):
    def __init__(self, path, flags, cwd=''):
        md5_hash = get_and_validate_md5_hash(path)

        def _read_file():
            with open(cache_file_path(md5_hash, cwd), flags) as f:
                return f.read()

        super().__init__(_read_file, '', hash=md5_hash)


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


def track_file(file_path, gitignore_path='.gitignore', cwd=''):
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
