import os
import shutil
import unittest
import merkl
from merkl.cli.init import InitAPI
from merkl.cache import SqliteCache


class TestCaseWithMerklRepo(unittest.TestCase):
    def setUp(self):
        os.makedirs('/tmp/', exist_ok=True)
        merkl.io.cwd = '/tmp/'
        api = InitAPI()
        api.init()

    def tearDown(self):
        shutil.rmtree('/tmp/.merkl/')
        merkl.io.cwd = None
        SqliteCache.connection = None


