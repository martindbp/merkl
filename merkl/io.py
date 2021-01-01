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
    def __init__(self, path):
        self.path = path
        merkl_path = path + '.merkl'
        if not os.path.exists(merkl_path):
            raise FileNotTrackedError

        with open(merkl_path, 'r') as f:
            self.hash = json.loads(f.read())['md5_hash']


def map_to_hash(val):
    if isinstance(val, Future):
        return {'merkl_hash': val.hash}
    elif isinstance(val, TrackedPath):
        return {'md5_hash': val.hash}
    elif not (isinstance(val, str) or isinstance(val, int) or isinstance(val, float)):
        raise NonSerializableArgError
    return val


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


def get_file_future(path, flags):
    merkl_file = path + '.merkl'
    if not os.path.exists(merkl_file):
        raise FileNotTrackedError

    with open(merkl_file) as f:
        md5_hash = json.load(f)['md5_hash']

    def _read_file():
        with open(cache_file(md5_hash), flags) as f:
            return f.read()

    return Future(
        fn=_read_file,
        output_hash=md5_hash,
    )


def get_fileobject_future(path, flags):
    merkl_file = path + '.merkl'
    if not os.path.exists(merkl_file):
        raise FileNotTrackedError

    with open(merkl_file) as f:
        md5_hash = json.load(f)['md5_hash']

    def _get_fileobject():
        return open(cache_file(md5_hash), flags)

    return Future(
        fn=_get_fileobject,
        output_hash=md5_hash,
    )
