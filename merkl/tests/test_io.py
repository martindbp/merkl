import os
import json
import pickle
import shutil
import unittest
from time import sleep
from pathlib import Path
import merkl.io
from merkl import *
from merkl.exceptions import FileNotTrackedError, TrackedFileNotUpToDateError


class TestIO(unittest.TestCase):
    tmp_file = '/tmp/merkl/tmpfile.txt'
    tmp_file2 = '/tmp/merkl/tmpfile2.txt'
    gitignore_file = '/tmp/merkl/.gitignore'
    cwd = '/tmp/merkl/'

    def setUp(self):
        os.makedirs('/tmp/merkl/', exist_ok=True)
        with open(self.tmp_file, 'w') as f:
            f.write('hello world')

        with open(self.tmp_file2, 'w') as f:
            f.write('hej världen')

        with open(self.gitignore_file, 'w') as f:
            f.write('testfile.txt')

        merkl.io.cwd = self.cwd

    def tearDown(self):
        shutil.rmtree('/tmp/merkl/')
        merkl.io.cwd = None

    def test_tracking(self):
        track_file(self.tmp_file, self.gitignore_file)
        self.assertTrue(os.path.exists(self.tmp_file))
        self.assertTrue(os.path.exists(self.tmp_file + '.merkl'))

        def _assert_gitignore():
            with open(self.gitignore_file, 'r') as f:
                lines = f.read().split('\n')
                self.assertEqual(len(lines), 2)
                self.assertEqual(lines[-1], self.tmp_file)

        _assert_gitignore()

        # Track same file again and make sure nothing changed
        track_file(self.tmp_file, self.gitignore_file)
        _assert_gitignore()

        with open(self.tmp_file + '.merkl') as f:
            md5_hash = json.loads(f.read())['md5_hash']

        # Update content and check that hash in .merkl file changed
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')

        track_file(self.tmp_file, self.gitignore_file)

        with open(self.tmp_file + '.merkl') as f:
            new_md5_hash = json.loads(f.read())['md5_hash']
            self.assertNotEqual(new_md5_hash, md5_hash)

        # Check that we get an exception if we try to use a tracked file that is not up to date with the actual file
        sleep(0.01)
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye cruel world')

        with self.assertRaises(TrackedFileNotUpToDateError):
            fread(self.tmp_file, '')

    def test_fread(self):
        with self.assertRaises(FileNotTrackedError):
            fread('non_existant_file.txt', '')

        track_file(self.tmp_file, self.gitignore_file)
        ff = fread(self.tmp_file, '')
        self.assertEqual(ff.eval(), 'hello world')

    def test_fwrite(self):
        @task
        def task1():
            return b'some data'

        out = fwrite(task1(), self.tmp_file)
        out.eval()

        with open(self.tmp_file, 'rb') as f:
            self.assertEqual(f.read(), pickle.dumps(b'some data'))

    def test_fpath(self):
        @task
        def task1(path):
            return path

        fpath1 = fpath(self.tmp_file)
        self.assertTrue(isinstance(fpath1, Future))
        self.assertEqual(task1(fpath1).eval(), self.tmp_file)

        track_file(self.tmp_file, self.gitignore_file)
        fpath2 = fpath(self.tmp_file)
        self.assertEqual(fpath1.hash, fpath2.hash)

        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')
        track_file(self.tmp_file, self.gitignore_file)
        fpath3 = fpath(self.tmp_file)

        self.assertNotEqual(fpath3.hash, fpath2.hash)


if __name__ == '__main__':
    unittest.main()
