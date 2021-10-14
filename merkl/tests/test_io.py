import os
import json
import pickle
import dill
import shutil
import unittest
from time import sleep
from pathlib import Path
from unittest.mock import patch

import merkl.io
from merkl import *
from merkl.tests import TestCaseWithMerklRepo
from merkl.utils import get_hash_memory_optimized
from merkl.io import migrate_output_files


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
        for path in [self.tmp_file, self.tmp_file + '.merkl', self.tmp_file2, self.tmp_file2 + '.merkl']:
            try:
                os.remove(path)
            except:
                pass

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

        with open(self.tmp_file + '.merkl', 'rb') as f:
            self.assertEqual(dill.load(f).hash, out.hash)

        # Make types other than strings raises errors
        with self.assertRaises(TypeError):
            out = task1() >> 3

    def test_output_files(self):
        @task
        def task1(data):
            return data

        out = task1(["data"])
        out_file = '/tmp/output_file.pickle'
        out >> out_file
        out.eval()

        self.assertTrue(os.path.exists(out_file))
        self.assertTrue(os.path.exists(out_file + '.merkl'))

        os.remove(out_file)
        os.remove(out_file + '.merkl')

        out = task1(["data"])
        out_file = '/tmp/output_file.pickle'
        out >> out_file
        out.eval()

        self.assertTrue(os.path.exists(out_file))
        self.assertTrue(os.path.exists(out_file + '.merkl'))
        with open(out_file+'.merkl', 'rb') as f:
            hash1 = dill.load(f).hash

        out = task1(["data2"])
        out_file = '/tmp/output_file.pickle'
        out >> out_file
        out.eval()

        with open(out_file+'.merkl', 'rb') as f:
            hash2 = dill.load(f).hash

        self.assertNotEqual(hash1, hash2)


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


    def test_output_file_migration(self):
        @task
        def task1(value):
            return f'some data {value}'

        def pipeline(value):
            out = task1(value)
            out = out >> self.tmp_file
            return out

        out = pipeline(1).eval()

        with open(self.tmp_file + '.merkl', 'rb') as f:
            hash1 = dill.load(f).hash

        out = pipeline(2)
        self.assertFalse(out.in_cache())

        migrate_output_files(out)

        with open(self.tmp_file + '.merkl', 'rb') as f:
            hash2 = dill.load(f).hash

        self.assertNotEqual(hash1, hash2)

        # Now test that the new migrated out file means that out is in cache
        self.assertTrue(out.in_cache())

        # Test migrating with glob
        out = pipeline(3)
        migrated_files = migrate_output_files(out, '/tmp/tmpfile.*')
        self.assertEqual(len(migrated_files), 1)

        out = pipeline(4)
        migrated_files = migrate_output_files(out, '/tmp/other_tmpfile.*')
        self.assertEqual(len(migrated_files), 0)

if __name__ == '__main__':
    unittest.main()
