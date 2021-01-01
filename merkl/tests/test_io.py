import os
import unittest
from merkl.io import get_file_future, get_fileobject_future, track_file
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


if __name__ == '__main__':
    unittest.main()
