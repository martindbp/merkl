import os
import json
import pickle
import shutil
import unittest
from time import sleep
from pathlib import Path
import merkl.io
from merkl import *
from merkl.exceptions import FileNotTrackedError


class TestIO(unittest.TestCase):
    tmp_file = '/tmp/merkl/tmpfile.txt'
    tmp_file2 = '/tmp/merkl/tmpfile2.txt'
    cwd = '/tmp/merkl/'

    def setUp(self):
        os.makedirs('/tmp/merkl/', exist_ok=True)
        with open(self.tmp_file, 'w') as f:
            f.write('hello world')

        with open(self.tmp_file2, 'w') as f:
            f.write('hej världen')

        merkl.io.cwd = self.cwd

    def tearDown(self):
        shutil.rmtree('/tmp/merkl/')
        merkl.io.cwd = None

    def test_read_future(self):
        with self.assertRaises(FileNotTrackedError):
            read_future('non_existant_file.txt', '')

        track_file(self.tmp_file)
        ff = read_future(self.tmp_file, '')
        self.assertEqual(ff.eval(), 'hello world')

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

    def test_path_future(self):
        @task
        def task1(path):
            return path

        fpath1 = path_future(self.tmp_file)
        self.assertTrue(isinstance(fpath1, Future))
        self.assertEqual(task1(fpath1).eval(), self.tmp_file)

        track_file(self.tmp_file)
        fpath2 = path_future(self.tmp_file)
        self.assertEqual(fpath1.hash, fpath2.hash)

        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')
        track_file(self.tmp_file)
        fpath3 = path_future(self.tmp_file)

        self.assertNotEqual(fpath3.hash, fpath2.hash)


if __name__ == '__main__':
    unittest.main()
