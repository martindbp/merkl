import os
import unittest
from pathlib import Path
from merkl import *
from merkl.exceptions import *
from merkl.tests import TestCaseWithMerklRepo
from merkl.io import FileOut, DirOut
from merkl.cache import get_cache_file_path, SqliteCache


class TestCache(TestCaseWithMerklRepo):
    def test_caching(self):
        task_has_run = False

        @task
        def task1(arg):
            nonlocal task_has_run
            task_has_run = True
            return arg

        val = task1('test').eval()
        self.assertEqual(val, 'test')
        # Make sure task_has_run was changed
        self.assertTrue(task_has_run)

        # Now reset and test that value is cached (task_has_run is not changed)
        task_has_run = False
        val = task1('test').eval()
        self.assertEqual(val, 'test')
        self.assertFalse(task_has_run)

    def test_file_outs(self):
        filename = '/tmp/merkl_test.txt'

        @task
        def my_task():
            with open(filename, 'w') as f:
                f.write('test')
            return FileOut(filename)

        out = my_task()
        out.eval()
        with open(get_cache_file_path(out.hash, 'txt'), 'r') as f:
            self.assertEqual(f.read(), 'test')

        self.assertTrue(os.path.exists(filename))
        # Check that file is removed with rm_after_caching=True is passed in

        @task
        def my_task():
            with open(filename, 'w') as f:
                f.write('test')
            return FileOut(filename, rm_after_caching=True)

        f = my_task().eval()
        self.assertFalse(os.path.exists(filename))
        self.assertNotEqual(f.path, filename)
        # Check that extension of file in cache is the same
        self.assertEqual(f.path.split('.')[-1], filename.split('.')[-1])

        self.assertTrue(my_task().in_cache())
        self.assertNotEqual(my_task().eval().path, filename)
        self.assertEqual(my_task().eval().path.split('.')[-1], 'txt')

        # Check that temporary unnamed files work
        @task
        def my_task():
            file_out = FileOut(ext='txt')
            with open(file_out.path, 'w') as f:
                f.write('test')
            return file_out

        path = my_task().eval().path
        self.assertTrue(path.endswith('.txt'))

        with open(path, 'r') as f:
            self.assertEqual(f.read(), 'test')

    def test_dir_outs(self):
        filenames = []

        @task
        def my_task():
            nonlocal filenames
            dir_out = DirOut()

            for i in range(5):
                filename = dir_out.get_new_file(ext='txt')
                filenames.append(filename)
                with open(filename, 'w') as f:
                    f.write(f'test{i}')

            return dir_out

        dir_out = my_task().eval()
        self.assertEqual(len(dir_out.files), 5)
        for i, (filename, orig_filename) in enumerate(zip(dir_out.files, filenames)):
            self.assertNotEqual(filename, orig_filename)
            with open(filename, 'r') as f:
                self.assertEqual(f.read(), f'test{i}')

        # Test caching
        dir_out = my_task()
        self.assertTrue(dir_out.in_cache())
        dir_out = dir_out.eval()
        for i, filename in enumerate(dir_out.files):
            with open(filename, 'r') as f:
                self.assertEqual(f.read(), f'test{i}')

        # Test load_files()
        @task
        def my_task():
            dir_out = DirOut()
            for i in range(5):
                filename = str(Path(dir_out.path) / f'file{i}.txt')
                with open(filename, 'w') as f:
                    f.write(f'test{i}')

            dir_out.load_files()
            self.assertEqual(len(dir_out.files), 5)

            return dir_out

        my_task().eval()

if __name__ == '__main__':
    unittest.main()
