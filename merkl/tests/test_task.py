import sys
import unittest
from io import StringIO

import merkl
from merkl.tests.tasks.embed_bert import embed_bert, embed_bert_large
from merkl.tests.tasks.embed_elmo import embed_elmo
from merkl.tests.tasks.find_deps_test import my_task
from merkl.tests import TestCaseWithMerklRepo
from merkl.future import Future
from merkl.task import task, batch, pipeline, HashMode
from merkl.exceptions import *
from merkl.utils import get_hash_memory_optimized
from merkl.io import path_future


def get_stderr(f):
    saved_stderr = sys.stderr
    try:
        err = StringIO()
        sys.stderr = err
        # Call the function
        out = f()
        err_output = err.getvalue().strip()
    finally:
        sys.stderr = saved_stderr

    return out, err_output


class TestTask(TestCaseWithMerklRepo):
    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    def test_task_hashing(self):
        # Test that hash is the same every time
        self.assertEqual(embed_bert('sentence').hash, embed_bert('sentence').hash)

        # Test that hash is different for different inputs
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert('another sentence').hash)

        # Test that hash is different with different function 
        self.assertNotEqual(embed_elmo('sentence').hash, embed_bert('sentence').hash)

        # Test that hash is different with different function in same file
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert_large('sentence').hash)

        # Test that we raise the right exception when trying to pass something non-serializable to a task
        class MyClass:
            pass

        with self.assertRaises(SerializationError):
            embed_bert(MyClass()).hash

    def test_outs(self):
        with self.assertRaises(TaskOutsError):  # <= 0
            @task(outs=0)
            def _task_zero_outs(input_value):
                return input_value, 3

        with self.assertRaises(TaskOutsError):  # <= 0
            @task(outs=-1)
            def _task_negative_outs(input_value):
                return input_value, 3

        with self.assertRaises(TaskOutsError):  # bad outs type
            @task(outs=1.0)
            def _task_float_outs(input_value):
                return input_value, 3

        @task
        def _task1(input_value):
            return input_value, 3

        outs = _task1('test')
        # Single output by default
        self.assertEqual(len(outs), 2)
        for out in outs:
            self.assertTrue(isinstance(out, Future))

        with self.assertRaises(TaskOutsError):  # num returns mismatch
            @task
            def _task1(input_value):
                if input_value > 4:
                    return input_value, 2, 3
                return input_value, 3

        # Mismatching return in nested should be ok (should not raise exception)
        @task
        def _task1(input_value):
            def _nested_function():
                return input_value, 2, 3
            return input_value, 3

        # Single return value of other type than tuple should just produce a single Future (except dict)
        @task
        def _task1(input_value):
            return [1, 2*input_value]

        out = _task1('test')
        self.assertTrue(isinstance(out, Future))

        # Now set outs to 2, so we get two separate futures
        @task(outs=2)
        def _task2(input_value):
            return input_value, 3

        outs = _task2('test')
        self.assertEqual(len(outs), 2)
        self.assertNotEqual(outs[0].hash, outs[1].hash)

        # Test `outs` as a function
        @task(outs=lambda input_value, k: k)
        def _task3(input_value, k):
            return input_value, 3

        outs = _task3('test', 4)
        self.assertEqual(len(outs), 4)

        # Test that the wrong function signature fails
        with self.assertRaises(TaskOutsError):
            @task(outs=lambda inpoot_value, k: k)
            def _task4(input_value, k):
                return input_value, 3

        # A differing default value should not raise error
        @task(outs=lambda input_value, *args, k: k)
        def _task4(input_value, *args, k=3):
            return input_value, 3

        # Check that we get error if we return wrong number of outs
        @task(outs=3)
        def _task5(input_value):
            return input_value, 3

        with self.assertRaises(TaskOutsError):
            a, b, c = _task5('test')
            a.eval()

        # Unless it's one out
        @task(outs=1)
        def _task5(input_value):
            return input_value, 3

        _task5('test').eval()

        # Test dicts
        @task
        def _task6(input_value):
            return {'out1': 3, 'out2': input_value}

        out = _task6(5)
        self.assertTrue(isinstance(out, dict))
        self.assertEqual(len(out), 2)
        self.assertEqual(set(out.keys()), {'out1', 'out2'})
        self.assertEqual(out['out1'].eval(), 3)
        self.assertEqual(out['out2'].eval(), 5)

        # Test that dicts with keys that are not string literals fail
        with self.assertRaises(TaskOutsError):
            @task
            def _task7(input_value):
                key = 'out1'
                return {key: 3, 'out2': input_value}

    def test_pipelines(self):
        @task(outs=lambda input_values: len(input_values))
        def _task1(input_values):
            return [val**2 for val in input_values]

        import math
        @task
        def _task2(input_value):
            return math.sqrt(input_value)

        vals = [1, 1, 1, 4, 5, 6]
        outs_stage1 = _task1(vals)
        outs_stage2 = [_task2(val) for val in outs_stage1]
        self.assertEqual([out.eval() for out in outs_stage2], vals)

        # Test that all hashes are different
        self.assertEqual(len(set(out.hash for out in outs_stage2)), len(vals))

    def test_hash_modes(self):
        def _task1(input_value):
            return input_value, 3

        with self.assertRaises(TypeError):
            _task1_file = task(hash_mode='not a hash mode')(_task1)

        _task1_file = task(hash_mode=HashMode.MODULE)(_task1)
        _task1_function = task(hash_mode=HashMode.FUNCTION)(_task1)
        for out1, out2 in zip(_task1_file('test'), _task1_function('test')):
            self.assertNotEqual(out1.hash, out2.hash)

        # Test that two identical functions in two different files (with slightly different content)
        # are the same if HashMode.FUNCTION is used
        from merkl.tests.tasks.identical_task1 import identical_task as identical_task1
        from merkl.tests.tasks.identical_task2 import identical_task as identical_task2
        self.assertEqual(identical_task1('test').hash, identical_task2('test').hash)

        def fn_dep1(arg1, arg2):
            return arg1 + arg2

        def fn_dep2(arg1, arg2):
            return arg1 + arg2

        from merkl.tests.tasks import embed_bert as embed_bert_module
        _task1_module_dep = task(deps=[embed_bert_module])(_task1)
        _task1_function_dep1 = task(deps=[fn_dep1])(_task1)
        _task1_function_dep2 = task(deps=[fn_dep2])(_task1)
        _task1_string_dep = task(deps=['val1'])(_task1)
        _task1_bytes_dep = task(deps=[b'val1'])(_task1)
        hashes = [
            _task1_module_dep('test')[0].hash,
            _task1_function_dep1('test')[0].hash,
            _task1_function_dep2('test')[0].hash,
            _task1_string_dep('test')[0].hash,
            _task1_bytes_dep('test')[0].hash,
        ]
        # Check that all hashes are unique, except the last two which are equal
        self.assertEqual(len(set(hashes[:3])), 3)
        self.assertEqual(len(set(hashes[3:])), 1)

        deps = [name for (name, dep) in my_task.deps]
        self.assertEqual(len(deps), 2)
        self.assertTrue('my_global_variable' in deps)
        self.assertTrue('_my_fn' in deps)

        # Test path_future as dep
        filepath = '/tmp/my_file.txt'
        with open(filepath, 'w') as f:
            f.write('test')

        @task(deps=[path_future(filepath)])
        def my_dep_task():
            return 1

        self.assertTrue(f'<Future {get_hash_memory_optimized(filepath)}>' in my_dep_task.deps)


    def test_future_operator_access(self):
        # Test that Future cannot be accessed by checking some operators
        future = embed_bert('sentence')
        with self.assertRaises(FutureAccessError):
            # Can't be added to set
            set([future])

        with self.assertRaises(FutureAccessError):
            # Can't be used as a truth value in statements
            if future:
                print('hello world')

        with self.assertRaises(FutureAccessError):
            # Can't be iterated over
            for x in future:
                print('hello world')

        with self.assertRaises(FutureAccessError):
            # Can't get a length from it
            print(len(future))

        with self.assertRaises(FutureAccessError):
            future += 1

    def test_batch_tasks(self):
        def fun(arg):
            return 3

        with self.assertRaises(BatchTaskError):
            # `fun` is not decorated
            @batch(fun)
            def fun2(args):
                pass

        @task
        def fun3(arg):
            return 1, 3

        with self.assertRaises(BatchTaskError):
            # batch function should only have an 'args' argument
            @batch(fun3)
            def fun4(args, other_arg):
                pass

        _embed_bert = embed_bert.__wrapped__

        called = 0
        num_inputs = None

        @batch(embed_bert)
        def embed_bert_batch(args):
            nonlocal called, num_inputs
            called += 1
            num_inputs = len(args)
            return [_embed_bert(arg) for arg in args]

        with self.assertRaises(BatchTaskError):
            embed_bert_batch('test1')  # not a list

        batch_outs = embed_bert_batch(['test1', 'test2', 'test3', 'test1', 'test4', 'test5'])
        single_outs = [embed_bert(arg) for arg in ['test1', 'test2', 'test3', 'test1']]
        for single_out, batch_out in zip(single_outs[:-1], batch_outs[:-1]):
            self.assertEqual(single_out.hash, batch_out.hash)
            self.assertEqual(single_out.eval(), batch_out.eval())

        self.assertEqual(batch_outs[0].hash, batch_outs[-3].hash)
        # test1-3 should have been cached from the evaluation of the single function
        self.assertEqual(called, 0)
        self.assertEqual(num_inputs, None)

        # Now if we evaluate test5, it should be called
        batch_outs = embed_bert_batch(['test1', 'test2', 'test3', 'test1', 'test4', 'test5'])
        batch_outs[-1].eval()
        self.assertEqual(called, 1)
        self.assertEqual(num_inputs, 2)

        num_inputs = None
        called = 0
        batch_outs = embed_bert_batch(['test1', 'test2', 'test3', 'test1', 'test6'])
        for out in batch_outs:
            res = out.eval()

        # This tests is for a bug that happened in caching
        self.assertEqual(batch_outs[0].eval(), ['t', '1'])

        self.assertEqual(called, 1)
        self.assertEqual(num_inputs, 1)

        called = 0

        @task(outs=1)
        def single(inpt):
            raise NotImplementedError

        @batch(single)
        def embed_bert_without_single(args):
            nonlocal called
            called += 1
            return [_embed_bert(arg) for arg in args]

        batch_outs_without_single = embed_bert_without_single(['test1', 'test2', 'test3', 'test1'])
        for batch_out, batch_out_without_single in zip(batch_outs, batch_outs_without_single):
            self.assertNotEqual(batch_out.hash, batch_out_without_single.hash)
            self.assertEqual(batch_out_without_single.eval(), batch_out.eval())

        self.assertEqual(called, 1)

        @task
        def single_multiple_outs(arg1, arg2):
            return (arg1*2, arg2*3)

        @batch(single_multiple_outs)
        def batch_with_multiple_outs(args):
            return [(arg1*2, arg2*3) for arg1, arg2 in args]

        outs = batch_with_multiple_outs([(1,2), (2,3), (3,4), (1,2)])
        self.assertEqual(outs[0][0].hash, outs[-1][0].hash)
        self.assertEqual(outs[0][1].hash, outs[-1][1].hash)

        self.assertEqual(outs[0][0].eval(), outs[-1][0].eval())
        for out in outs:
            out[0].eval()
            out[1].eval()

        # Test caching
        outs = batch_with_multiple_outs([(1,2), (2,3), (3,4), (1,2)])
        for out in outs:
            self.assertTrue(out[0].in_cache())
            self.assertTrue(out[1].in_cache())

        self.assertEqual(outs[0][0].eval(), 2)
        self.assertEqual(outs[0][1].eval(), 6)

        @task
        def single_one_arg(arg):
            return (arg*2, arg2*3)

        @batch(single_one_arg)
        def batch_two_args(args):
            return [(arg1*2, arg2*3) for arg1, arg2 in args]

        with self.assertRaises(TypeError):
            batch_two_args([(1,2), (2,3)])


    def test_pipelines(self):
        @task(cache=None)
        def my_task(k):
            return k*k

        with self.assertRaises(SerializationError):
            # my_task has no cache set, so caching the pipeline result
            # would not cache the nested Future result

            @pipeline
            def my_pipeline(i):
                return my_task(i)

            my_pipeline(3)

        @task
        def my_other_task(m):
            return m ** 2

        @pipeline
        def my_pipeline(i):
            return my_other_task(my_task(i))

        fut1 = my_pipeline(2)
        self.assertFalse(fut1.in_cache())
        self.assertEqual(fut1.eval(), 16)

        fut2 = my_pipeline(2)
        self.assertTrue(fut2.in_cache())
        self.assertEqual(fut2.hash, fut1.hash)
        self.assertEqual(fut2.parent_futures(), [])

    def test_pipe_syntax(self):
        @task
        def task1():
            return 1

        @task
        def task2(arg):
            return 2*arg

        res1 = task1() | task2
        res2 = task2(task1())
        self.assertEqual(res1.hash, res2.hash)


if __name__ == '__main__':
    unittest.main()
