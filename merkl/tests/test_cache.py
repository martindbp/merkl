import os
import math
import unittest
from pathlib import Path
from merkl import *
from merkl.exceptions import *
from merkl.tests import TestCaseWithMerklRepo
from merkl.io import FileRef, DirRef
from merkl.cache import get_cache_file_path, SqliteCache, BLOB_DB_SIZE_LIMIT_BYTES, MEMORY_CACHE
from merkl.utils import evaluate_futures, Eval
from merkl.util_tasks import combine_file_refs


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
            return FileRef(filename)

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
            return FileRef(filename, rm_after_caching=True)

        f = my_task().eval()
        self.assertFalse(os.path.exists(filename))
        self.assertNotEqual(f, filename)
        # Check that extension of file in cache is the same
        self.assertEqual(f.split('.')[-1], filename.split('.')[-1])

        self.assertTrue(my_task().in_cache())
        self.assertNotEqual(my_task().eval(), filename)
        self.assertEqual(my_task().eval().split('.')[-1], 'txt')

        # Check that temporary unnamed files work
        @task
        def my_task():
            global filename
            file_out = FileRef(ext='txt')
            filename = str(file_out)
            with open(file_out, 'w') as f:
                f.write('test')
            return file_out

        path = my_task().eval()
        self.assertTrue(path.endswith('.txt'))
        self.assertNotEqual(str(path), filename)
        self.assertFalse(os.path.exists(filename))  # temporary file should have been deleted

        with open(path, 'r') as f:
            self.assertEqual(f.read(), 'test')

        @task
        def my_task_single(arg):
            return arg, arg

        # Had a problem where both FileRefs from a batch task had same hash
        @batch(my_task_single)
        def my_task(args):
            files = []
            for arg in args:
                a = FileRef()
                with open(a, 'w') as f:
                    f.write('{arg} a')
                b = FileRef()
                with open(b, 'w') as f:
                    f.write('{arg} b')
                files.append((a, b))
            return files

        files = my_task(['arg1', 'arg2', 'arg3'])
        for file_a, file_b in files:
            self.assertNotEqual(file_a.hash, file_b.hash)
            file_a_out = file_a.eval()
            file_b_out = file_b.eval()
            self.assertNotEqual(file_a_out, file_b_out)

            # Make sure the files get deleted after clearing cache
            self.assertTrue(os.path.exists(file_a_out))
            file_a.clear_cache()
            self.assertFalse(os.path.exists(file_a_out))

    def test_dir_outs(self):
        filenames = []

        dir_path = None

        @task
        def my_task():
            nonlocal filenames, dir_path
            dir_out = DirRef()
            dir_path = str(dir_out)

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

        self.assertNotEqual(str(dir_out), dir_path)
        self.assertFalse(os.path.exists(dir_path))  # temporary dir should have been deleted

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
            dir_out = DirRef()
            for i in range(5):
                filename = str(Path(dir_out) / f'file{i}.txt')
                with open(filename, 'w') as f:
                    f.write(f'test{i}')

            dir_out.load_files()
            self.assertEqual(len(dir_out.files), 5)

            return dir_out

        my_task().eval()

        # Test combine_file_refs / DirRef.link_file_ref_into_dir()
        num_files = 5

        @task(outs=num_files)
        def my_task():
            refs = []
            for i in range(num_files):
                file_ref = FileRef()
                with open(file_ref, 'w') as f:
                    f.write(str(i))
                refs.append(file_ref)
            return refs

        file_refs = my_task()
        dir_ref = combine_file_refs(file_refs)
        dir_ref = dir_ref.eval()
        self.assertEqual(len(dir_ref.files), num_files)
        self.assertTrue(os.path.exists(file_refs[0].eval()))
        dir_file_path = Path(dir_ref) / file_refs[0].eval()
        self.assertTrue(os.path.exists(dir_file_path))
        with open(dir_file_path, 'r') as f:
            f1 = f.read()

        with open(file_refs[0].eval(), 'r') as f:
            f2 = f.read()

        self.assertEqual(f1, f2)

    def test_future_serialization(self):
        @task
        def my_task():
            return 3

        future = my_task()
        serialized = future.serializer.dumps(future)
        unserialized = future.serializer.loads(serialized)
        self.assertEqual(unserialized.serializer, future.serializer)

    def test_none_inputs(self):
        # This would break before when I did a manual check for value type serializability, leaving it here just in case
        @task
        def my_task(inpt=None):
            return inpt

        my_task().eval()

    def test_cache_temporarily(self):
        @task
        def my_task(a):
            return 2*a

        a = my_task(0)
        a.cache_temporarily = True
        a.eval()
        self.assertTrue(a.in_cache())
        b = my_task(a)
        b.eval()
        self.assertFalse(a.in_cache())
        self.assertTrue(b.in_cache())

    def test_cache_larger_blobs(self):
        # First test that small blob doesn't get stored in file
        hash = 'gfafasf323'
        SqliteCache.add(hash, b'small blob')
        self.assertFalse(os.path.exists(get_cache_file_path(hash)))

        # Then check that large blog is stored in file
        hash = 'gy25uhg2hwy34'
        big_blob = b'big blob!!'
        big_blob = big_blob * (math.ceil(BLOB_DB_SIZE_LIMIT_BYTES / len(big_blob)) + 1)  # make it large
        SqliteCache.add(hash, big_blob)
        self.assertTrue(os.path.exists(get_cache_file_path(hash)))

        # Check that file is removed when cache is cleared
        SqliteCache.clear(hash)
        self.assertFalse(os.path.exists(get_cache_file_path(hash)))

    def test_siblings_evaluated(self):
        # There's an issue where if a function has multiple outs, and only some of them have been evaluated before the
        # program crashes for some reason, then those outs that were not evaluated are never cached, even though the
        # result is in the shared cache. Therefore, changed it so that all sibling futures get evaluated as well, so
        # test that here
        @task
        def my_task():
            return 1, 2, 3

        @task
        def my_second_task(val):
            if val != 2:
                return val
            else:
                raise KeyboardInterrupt

        outs1 = my_task()
        outs2 = [my_second_task(out) for out in outs1]
        outs2[0].eval()
        with self.assertRaises(KeyboardInterrupt):
            outs2[1].eval()

        # Eval-ing out22 failed, so outs1[2] was never reached, but all outs from first stage should be cached
        for out in outs1:
            self.assertTrue(out.in_cache())

    def test_clear_all_except(self):
        @task
        def my_task(val):
            return 3, b'xxxxxxxxxx' * 20000  # something above the 100k limit for storing directly in DB

        out1, out2 = my_task(2)
        out1.eval()
        # Some sanity checks
        self.assertFalse(os.path.exists(get_cache_file_path(out1.hash)))
        self.assertTrue(os.path.exists(get_cache_file_path(out2.hash)))
        self.assertTrue(SqliteCache.has(out1.hash))
        self.assertTrue(SqliteCache.has(out2.hash))

        # Now let's say we evaluate my_task with some other input
        out1_2, out2_2 = my_task(3)
        out1_2.eval()
        self.assertTrue(SqliteCache.has(out2_2.hash))

        # Now clear all except this out
        SqliteCache.clear_all_except([out1_2.hash])

        # None of these should be cached or on file
        self.assertFalse(SqliteCache.has(out2_2.hash))
        self.assertFalse(SqliteCache.has(out1.hash))
        self.assertFalse(SqliteCache.has(out2.hash))
        self.assertFalse(os.path.exists(get_cache_file_path(out2.hash)))

        # Make sure pipeline outs remain (there was a bug with this)
        @pipeline
        def my_pipeline():
            return my_task(3)

        out1, out2 = my_pipeline()
        out1.eval()

        cache.clear([out1], keep=True)
        self.assertTrue(SqliteCache.has(out1.hash))
        self.assertTrue(SqliteCache.has(out1.parent_pipeline_future.hash))

    def test_pipeline_cache_invalidation(self):
        # Test that a cached pipeline out is removed/recalculated if it contains a future which has not been evaled/cached

        @task
        def my_task(val):
            return 2*val

        called = False

        @pipeline
        def my_pipeline():
            nonlocal called
            called = True
            return my_task(3)

        out = my_pipeline() # this should cache the Future output from my_task, but we never evaluate it
        self.assertTrue(called)

        called = False
        out = my_pipeline()
        self.assertTrue(called)

        out.eval()
        called = False
        out = my_pipeline()
        self.assertFalse(called)

    def test_memory_caching(self):
        @task(cache_in_memory=True)
        def my_task(val):
            return 2*val

        out = my_task(2)
        self.assertEqual(len(MEMORY_CACHE), 0)
        out.eval()
        self.assertEqual(len(MEMORY_CACHE), 1)
        self.assertEqual(MEMORY_CACHE[out.hash], 4)

    def test_stats(self):
        SqliteCache.add('h1', b'123', fn_name='module1.function1')
        SqliteCache.add('h2', b'12345', fn_name='module1.function1')

        SqliteCache.add('h3', b'12', fn_name='module1.function2')
        SqliteCache.add('h4', b'1234', fn_name='module1.function2')
        stats = SqliteCache.get_stats()
        self.assertEqual(stats[0], ('module1.function1', 2, 3+5))
        self.assertEqual(stats[1], ('module1.function2', 2, 2+4))

        self.assertEqual(SqliteCache.get_stats('module1.function1'), (2, 3+5))

        # Test that large blobs stored on the filesystem are counted correctly
        SqliteCache.add('h5', b'12'*BLOB_DB_SIZE_LIMIT_BYTES, fn_name='module1.function3')
        self.assertEqual(SqliteCache.get_stats('module1.function3'), (1, 2*BLOB_DB_SIZE_LIMIT_BYTES))

        # Test that FileRefs returned from tasks are counted correctly
        num_bytes = 32
        @task
        def my_task():
            file_ref = FileRef()
            with open(file_ref, 'w') as f:
                f.write('a'*num_bytes)
            return file_ref

        my_task().eval()
        self.assertEqual(SqliteCache.get_stats('test_cache.my_task'), (1, num_bytes))

    def test_batch_stats(self):
        @task
        def my_task(val):
            return 2*val

        @batch(my_task)
        def my_batch_task(vals):
            return [2*val for val in vals]

        with Eval():
            my_batch_task([1,2,3])

        stats = SqliteCache.get_stats()
        # Test that it's reported as coming from the single fn
        self.assertEqual(stats[0][0], 'test_cache.my_task')

if __name__ == '__main__':
    unittest.main()
