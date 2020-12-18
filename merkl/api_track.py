import os
import json
import pathlib
from datetime import datetime
from merkl.utils import get_hash_memory_optimized

class TrackAPI:
    def track(self, files):
        """
        1. Hash the file
        2. Create a new file `<file>.merkl` containing the file hash and timestamp
        3. Hard link the file to `.merkl/cache`
        4. Add `<file>` to `.gitignore` if there is one
        """
        if not os.path.exists('.merkl/'):
            print('There is no .merkl/ directory here. Try running `merkl init` first.')
            exit(1)

        gitignore_exists = os.path.exists('.gitignore')

        for file_path in files:
            md5_hash = get_hash_memory_optimized(file_path, mode='md5')
            file_path = pathlib.Path(file_path)
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)

            merkl_file_path = str(file_path) + '.merkl'
            with open(merkl_file_path, 'w') as f:
                json.dump({
                    'hash': md5_hash,
                    'modified_timestamp': mtime.isoformat(),
                }, f, sort_keys=True, indent=4)

            cache_dir = f'.merkl/cache/{md5_hash[:2]}'
            os.makedirs(cache_dir, exist_ok=True)

            cache_file = f'{cache_dir}/{md5_hash}'
            try:
                os.link(str(file_path), cache_file)
            except FileExistsError:
                pass

            if gitignore_exists:
                with open('.gitignore', 'a') as f:
                    f.write(str(file_path) + '\n')



