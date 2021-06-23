import os
import json
import pickle
import shutil
import unittest
from time import sleep
from pathlib import Path
from unittest.mock import patch

import merkl.io
from merkl import *
from merkl.tests import TestCaseWithMerklRepo
from merkl.utils import get_hash_memory_optimized


class TestIO(TestCaseWithMerklRepo):
    tmp_file = '/tmp/tmpfile.txt'
    tmp_file2 = '/tmp/tmpfile2.txt'

    def setUp(self):
        super().setUp()
        with open(self.tmp_file, 'w') as f:
            f.write('hello world')

        with open(self.tmp_file2, 'w') as f:
            f.write('hej världen')

    def tearDown(self):
        super().tearDown()

    def test_read_future(self):
        with self.assertRaises(FileNotFoundError):
            read_future('non_existant_file.txt', '')

        called = False
        def _mock(path, mode):
            nonlocal called
            called = True
            return get_hash_memory_optimized(path, mode)

        with patch('merkl.io.get_hash_memory_optimized', _mock):
            ff = read_future(self.tmp_file, '')
            self.assertEqual(ff.eval(), 'hello world')

        self.assertTrue(called)
        called = False

        # Make sure this expensive hashing doesn't happen again the second time
        with patch('merkl.io.get_hash_memory_optimized', _mock):
            ff = read_future(self.tmp_file, '')

        self.assertFalse(called)

    def test_write_future(self):
        @task
        def task1():
            return b'some data'

        out = write_future(task1(), self.tmp_file)
        out.eval()

        with open(self.tmp_file, 'rb') as f:
            self.assertEqual(f.read(), pickle.dumps(b'some data'))

    def test_write_future_pipe_syntax(self):
        @task
        def task1():
            return b'some data2'

        @task
        def identity(arg):
            return arg

        out = task1() | identity > self.tmp_file
        out.eval()

        with open(self.tmp_file, 'rb') as f:
            self.assertEqual(f.read(), pickle.dumps(b'some data2'))

        # Make types other than strings raises errors
        with self.assertRaises(TypeError):
            out = task1() > 3

    def test_write_future_with_merkl_file(self):
        @task
        def task1():
            return b'some data2'

        out = task1()
        out = out >> self.tmp_file
        out.eval()

        with open(self.tmp_file, 'rb') as f:
            self.assertEqual(f.read(), pickle.dumps(b'some data2'))

        with open(self.tmp_file + '.merkl', 'r') as f:
            self.assertEqual(json.loads(f.read())['merkl_hash'], out.hash)

        # Make types other than strings raises errors
        with self.assertRaises(TypeError):
            out = task1() >> 3

    def test_path_future(self):
        @task
        def task1(path):
            return path

        fpath1 = path_future(self.tmp_file)
        self.assertTrue(isinstance(fpath1, Future))
        self.assertEqual(task1(fpath1).eval(), self.tmp_file)

        fpath2 = path_future(self.tmp_file)
        self.assertEqual(fpath1.hash, fpath2.hash)

        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')
        fpath3 = path_future(self.tmp_file)

        self.assertNotEqual(fpath3.hash, fpath2.hash)


if __name__ == '__main__':
    unittest.main()
