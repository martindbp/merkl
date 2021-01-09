import os
from merkl.io import track_file


class TrackAPI:
    def track(self, files):
        if not os.path.exists('.merkl/'):
            print('There is no .merkl/ directory here. Try running `merkl init` first.')
            exit(1)

        if not os.path.exists('.gitignore'):
            print("WARNING: no .gitignore file present, can't add tracked file to ignore list")

        for file_path in files:
            if file_path.endswith('.merkl'):
                continue

            track_file(file_path)
