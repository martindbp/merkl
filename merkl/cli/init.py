import os
import sqlite3

from merkl.cache import get_db_path, SqliteCache

class InitAPI:
    def init(self):
        if os.path.exists(get_db_path()):
            print('Repository .merkl already exists')
            return

        SqliteCache.create_cache()
