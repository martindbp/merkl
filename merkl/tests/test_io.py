import os
import json
import cloudpickle
import shutil
import unittest
from time import sleep
from pathlib import Path
from merkl import *
import merkl.io
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
            mread(self.tmp_file, '')

    def test_mread(self):
        with self.assertRaises(FileNotTrackedError):
            mread('non_existant_file.txt', '')

        track_file(self.tmp_file, self.gitignore_file)
        ff = mread(self.tmp_file, '')
        self.assertEqual(ff.eval(), 'hello world')

    def test_mopen(self):
        track_file(self.tmp_file, self.gitignore_file)
        fof = mopen(self.tmp_file, '')
        with fof.eval() as f:
            self.assertEqual(f.read(), 'hello world')

    def test_mwrite(self):
        @task
        def task1():
            return b'some data'

        out = mwrite(task1(), self.tmp_file)
        out.eval()

        with open(self.tmp_file, 'rb') as f:
            self.assertEqual(f.read(), cloudpickle.dumps(b'some data'))

    def test_path(self):
        self.assertNotEqual(FilePath(self.tmp_file).hash, FilePath(self.tmp_file2).hash)

    def test_tracked_path(self):
        track_file(self.tmp_file, self.gitignore_file)

        @task
        def task1(path):
            # .. read file
            return 1

        # Check that hash changes if we update the file content
        out1 = task1(TrackedFilePath(self.tmp_file))
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')
        track_file(self.tmp_file, self.gitignore_file)
        out2 = task1(TrackedFilePath(self.tmp_file))

        self.assertNotEqual(out1.hash, out2.hash)

    def test_tracked_paths(self):
        track_file(self.tmp_file, self.gitignore_file)
        track_file(self.tmp_file2, self.gitignore_file)

        @task
        def task1():
            return 1

        @task(deps=[TrackedFilePath(self.tmp_file)])
        def task2():
            return 1

        tracked_paths = TrackedFilePath.get_dir_paths('/tmp/merkl/')
        self.assertEqual(len(tracked_paths), 2)

        @task(deps=tracked_paths)
        def task3():
            return 1

        out1 = task1()
        out2 = task2()
        out3 = task3()
        self.assertNotEqual(out1.hash, out2.hash)
        self.assertNotEqual(out2.hash, out3.hash)


if __name__ == '__main__':
    unittest.main()
