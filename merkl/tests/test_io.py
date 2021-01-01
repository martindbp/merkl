import os
import json
import unittest
from pathlib import Path
from merkl import *
from merkl.exceptions import FileNotTrackedError


class TestIO(unittest.TestCase):
    tmp_file = '/tmp/tmpfile.txt'
    gitignore_file = '/tmp/.gitignore'

    def setUp(self):
        with open(self.tmp_file, 'w') as f:
            f.write('hello world')

        with open(self.gitignore_file, 'w') as f:
            f.write('testfile.txt')

    def tearDown(self):
        os.remove(self.tmp_file)
        os.remove(self.gitignore_file)
        if os.path.exists(self.tmp_file + '.merkl'):
            os.remove(self.tmp_file + '.merkl')

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

    def test_file_future(self):
        with self.assertRaises(FileNotTrackedError):
            get_file_future('non_existant_file.txt', 'r')

        track_file(self.tmp_file, self.gitignore_file)
        ff = get_file_future(self.tmp_file, 'r')
        self.assertEqual(ff.get(), 'hello world')

    def test_fileobject_future(self):
        track_file(self.tmp_file, self.gitignore_file)
        fof = get_fileobject_future(self.tmp_file, 'r')
        with fof.get() as f:
            self.assertEqual(f.read(), 'hello world')

    def test_tracked_path(self):
        track_file(self.tmp_file, self.gitignore_file)

        @task
        def task1(path):
            # .. read file
            return 1

        # Check that hash changes if we update the file content
        out1 = task1(TrackedPath(self.tmp_file))
        with open(self.tmp_file, 'w') as f:
            f.write('goodbye world')
        track_file(self.tmp_file, self.gitignore_file)
        out2 = task1(TrackedPath(self.tmp_file))

        self.assertNotEqual(out1.hash, out2.hash)


if __name__ == '__main__':
    unittest.main()
