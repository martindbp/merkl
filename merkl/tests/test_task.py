import sys
import unittest
from io import StringIO

from merkl.tests.tasks.embed_bert import embed_bert, embed_bert_large
from merkl.tests.tasks.embed_elmo import embed_elmo
from merkl.tests.tasks.cluster import cluster
from merkl.future import Future
from merkl.task import task, HashMode
from merkl.exceptions import *


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


class TestTask(unittest.TestCase):
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

        with self.assertRaises(NonSerializableArgError):
            embed_bert(MyClass())

    def test_outs(self):
        with self.assertRaises(NonPositiveOutsError):
            @task(outs=0)
            def _task_zero_outs(input_value):
                return input_value, 3

        with self.assertRaises(NonPositiveOutsError):
            @task(outs=-1)
            def _task_negative_outs(input_value):
                return input_value, 3

        with self.assertRaises(BadOutsValueError):
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

        with self.assertRaises(NumReturnValuesMismatchError):
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

        # Single return value of other type than tuple should just produce a single Future
        @task
        def _task1(input_value):
            return {'key1': 1, 'key2': input_value*2}

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
        with self.assertRaises(NonMatchingSignaturesError):
            @task(outs=lambda inpoot_value, k: k)
            def _task4(input_value, k):
                return input_value, 3

        # Check that we get error if we return wrong number of outs
        @task(outs=1)
        def _task5(input_value):
            return input_value, 3

        with self.assertRaises(WrongNumberOfOutsError):
            _task5('test').get()

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
        self.assertEqual([out.get() for out in outs_stage2], vals)

        # Test that all hashes are different
        self.assertEqual(len(set(out.hash for out in outs_stage2)), len(vals))

    def test_hash_modes(self):
        def _task1(input_value):
            return input_value, 3

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


if __name__ == '__main__':
    unittest.main()
