import os
import pathlib
import json
from datetime import datetime
from merkl.future import Future
from merkl.exceptions import FileNotTrackedError, NonSerializableArgError
from merkl.utils import get_hash_memory_optimized


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
            tracked_paths.append(TrackedPath(str(path).rstrip('.merkl')))
        return tracked_paths

    def __repr__(self):
        return f'TrackedPath: {self.hash}'


def map_to_hash(val):
    if hasattr(val, 'hash'):
        # Futures and TrackedPaths has 'hash' attribute
        return {'_hash': val.hash}
    elif not (isinstance(val, str) or isinstance(val, int) or isinstance(val, float)):
        raise NonSerializableArgError
    return val


class FileObjectFuture(Future):
    def __init__(self, path, flags):
        merkl_file = path + '.merkl'
        if not os.path.exists(merkl_file):
            raise FileNotTrackedError

        with open(merkl_file) as f:
            output_hash = json.load(f)['md5_hash']

        def _get_file_object():
            return open(cache_file(output_hash), flags)

        super().__init__(_get_file_object, output_hash)


class FileContentFuture(Future):
    def __init__(self, path, flags):
        merkl_file = path + '.merkl'
        if not os.path.exists(merkl_file):
            raise FileNotTrackedError

        with open(merkl_file) as f:
            output_hash = json.load(f)['md5_hash']

        def _read_file():
            with open(cache_file(output_hash), flags) as f:
                return f.read()


        super().__init__(_read_file, output_hash)


def track_file(file_path, gitignore_path='.gitignore'):
    """
    1. Hash the file
    2. Create a new file `<file>.merkl` containing the file hash and timestamp
    3. Hard link the file to `.merkl/cache`
    4. Add `<file>` to `.gitignore` if there is one
    """
    gitignore_exists = os.path.exists('.gitignore')
    merkl_exists = os.path.exists('.merkl')
    md5_hash = get_hash_memory_optimized(file_path, mode='md5')
    file_path = pathlib.Path(file_path)
    mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

    merkl_file_path = str(file_path) + '.merkl'
    with open(merkl_file_path, 'w') as f:
        json.dump({
            'md5_hash': md5_hash,
            'modified_timestamp': mtime.isoformat(),
        }, f, sort_keys=True, indent=4)

    if merkl_exists:
        os.makedirs(cache_dir(md5_hash), exist_ok=True)

        try:
            os.link(str(file_path), cache_file(md5_hash))
        except FileExistsError:
            pass

    if gitignore_exists:
        with open(gitignore_path, 'r') as f:
            lines = set(f.readlines())

        if str(file_path) not in lines:
            with open(gitignore_path, 'a') as f:
                f.write('\n' + str(file_path))


def cache_dir(md5_hash):
    return f'.merkl/cache/{md5_hash[:2]}'


def cache_file(md5_hash):
    return f'{cache_dir(md5_hash)}/{md5_hash}'
