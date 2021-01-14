import os
import json
import shutil
import unittest
from time import sleep
from pathlib import Path
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

    def tearDown(self):
        shutil.rmtree('/tmp/merkl/')

    def test_tracking(self):
        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)
        self.assertTrue(os.path.exists(self.tmp_file))
        self.assertTrue(os.path.exists(self.tmp_file + '.merkl'))

        def _assert_gitignore():
            with open(self.gitignore_file, 'r') as f:
                lines = f.read().split('\n')
                self.assertEqual(len(lines), 2)
                self.assertEqual(lines[-1], self.tmp_file)

        _assert_gitignore()

        # Track same file again and make sure nothing changed
        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)
        _assert_gitignore()

        with open(self.tmp_file + '.merkl') as f:
            md5_hash = json.loads(f.read())['md5_hash']

        # Update content and check that hash in .merkl file changed
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')

        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)

        with open(self.tmp_file + '.merkl') as f:
            new_md5_hash = json.loads(f.read())['md5_hash']
            self.assertNotEqual(new_md5_hash, md5_hash)

        # Check that we get an exception if we try to use a tracked file that is not up to date with the actual file
        sleep(0.01)
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye cruel world')

        with self.assertRaises(TrackedFileNotUpToDateError):
            FileContentFuture(self.tmp_file, 'r')

    def test_file_content_future(self):
        with self.assertRaises(FileNotTrackedError):
            FileContentFuture('non_existant_file.txt', 'r', self.cwd)

        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)
        ff = FileContentFuture(self.tmp_file, 'r', self.cwd)
        self.assertEqual(ff.get(), 'hello world')

    def test_file_object_future(self):
        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)
        fof = FileObjectFuture(self.tmp_file, 'r', self.cwd)
        with fof.get() as f:
            self.assertEqual(f.read(), 'hello world')

    def test_path(self):
        self.assertNotEqual(FilePath(self.tmp_file).hash, FilePath(self.tmp_file2).hash)

    def test_tracked_path(self):
        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)

        @task
        def task1(path):
            # .. read file
            return 1

        # Check that hash changes if we update the file content
        out1 = task1(TrackedFilePath(self.tmp_file))
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')
        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)
        out2 = task1(TrackedFilePath(self.tmp_file))

        self.assertNotEqual(out1.hash, out2.hash)

    def test_tracked_paths(self):
        track_file(self.tmp_file, self.gitignore_file, cwd=self.cwd)
        track_file(self.tmp_file2, self.gitignore_file, cwd=self.cwd)

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
