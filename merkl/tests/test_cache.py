import os
import unittest
from merkl import *
from merkl.exceptions import *
from merkl.tests import TestCaseWithMerklRepo
from merkl.io import FileOut, FileOutSerializer
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

    def test_cached_file(self):
        filename = '/tmp/merkl_test.txt'

        @task(serializer=FileOutSerializer)
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

        @task(serializer=FileOutSerializer)
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

        # Check that we get an exception if serializer is not set

        @task
        def my_task():
            with open(filename, 'w') as f:
                f.write('test')
            return FileOut(filename, rm_after_caching=True)

        with self.assertRaises(SerializationError):
            my_task().eval()


if __name__ == '__main__':
    unittest.main()
