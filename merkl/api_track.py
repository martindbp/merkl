import os
from merkl.io import track_file


class TrackAPI:
    def track(self, files):
        if not os.path.exists('.merkl/'):
            print('There is no .merkl/ directory here. Try running `merkl init` first.')
            exit(1)

        for file_path in files:
            track_file(file_path)
