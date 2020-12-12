import sys
import unittest
from io import StringIO

from nodes.embed_bert import embed_bert, embed_bert_large
from nodes.embed_elmo import embed_elmo
from nodes.cluster import cluster
from merkl.graph import MerkLFuture, node, HashMode
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


class TestMerkL(unittest.TestCase):

    def test_node_hashing(self):
        # Test that hash is the same every time
        self.assertEqual(embed_bert('sentence').hash, embed_bert('sentence').hash)

        # Test that hash is different for different inputs
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert('another sentence').hash)

        # Test that hash is different with different function 
        self.assertNotEqual(embed_elmo('sentence').hash, embed_bert('sentence').hash)

        # Test that hash is different with different function in same file
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert_large('sentence').hash)

        # Test that we raise the right exception when trying to pass something non-serializable to a node
        class MyClass:
            pass

        with self.assertRaises(NonSerializableArgException):
            embed_bert(MyClass())

    def test_outs(self):
        with self.assertRaises(NonPositiveOutsException):
            @node(outs=0)
            def _node_zero_outs(input_value):
                return input_value, 3

        with self.assertRaises(NonPositiveOutsException):
            @node(outs=-1)
            def _node_negative_outs(input_value):
                return input_value, 3

        with self.assertRaises(BadOutsValueException):
            @node(outs=1.0)
            def _node_float_outs(input_value):
                return input_value, 3

        @node
        def _node1(input_value):
            return input_value, 3

        out = _node1('test')
        # Single output by default
        self.assertTrue(isinstance(out, MerkLFuture))

        # Make sure we print a warning when we try to access the value
        # (since we didn't specify number of outs explicitly, and output is a tuple)
        outs, err_output = get_stderr(lambda: out.get())
        self.assertTrue('WARNING' in err_output)

        self.assertEqual(len(outs), 2)
        self.assertEqual(outs[0], 'test')

        # Now set outs to 2, so we get two separate futures
        @node(outs=2)
        def _node2(input_value):
            return input_value, 3

        outs = _node2('test')
        self.assertEqual(len(outs), 2)
        self.assertNotEqual(outs[0].hash, outs[1].hash)

        # Test `outs` as a function
        @node(outs=lambda input_value, k: k)
        def _node3(input_value, k):
            return input_value, 3

        outs = _node3('test', 4)
        self.assertEqual(len(outs), 4)

        # Test that the wrong function signature fails
        with self.assertRaises(NonMatchingSignaturesException):
            @node(outs=lambda inpoot_value, k: k)
            def _node4(input_value, k):
                return input_value, 3

    def test_pipelines(self):
        @node(outs=lambda input_values: len(input_values))
        def _node1(input_values):
            return [val**2 for val in input_values]

        import math
        @node
        def _node2(input_value):
            return math.sqrt(input_value)

        vals = [1, 1, 1, 4, 5, 6]
        outs_stage1 = _node1(vals)
        outs_stage2 = [_node2(val) for val in outs_stage1]
        self.assertEqual([out.get() for out in outs_stage2], vals)

        # Test that all hashes are different
        self.assertEqual(len(set(out.hash for out in outs_stage2)), len(vals))

    def test_hash_modes(self):
        def _node1(input_value):
            return input_value, 3

        _node1_file = node(hash_mode=HashMode.MODULE)(_node1)
        _node1_function = node(hash_mode=HashMode.FUNCTION)(_node1)
        self.assertNotEqual(_node1_file('test').hash, _node1_function('test').hash)

        # Test that two identical functions in two different files (with slightly different content)
        # are the same if HashMode.FUNCTION is used
        from nodes.identical_node1 import identical_node as identical_node1
        from nodes.identical_node2 import identical_node as identical_node2
        self.assertEqual(identical_node1('test').hash, identical_node2('test').hash)

        def fn_dep1(arg1, arg2):
            return arg1 + arg2

        def fn_dep2(arg1, arg2):
            return arg1 + arg2

        from nodes import embed_bert as embed_bert_module
        _node1_module_dep = node(deps=[embed_bert_module])(_node1)
        _node1_function_dep1 = node(deps=[fn_dep1])(_node1)
        _node1_function_dep2 = node(deps=[fn_dep2])(_node1)
        _node1_string_dep = node(deps=['val1'])(_node1)
        _node1_bytes_dep = node(deps=[b'val1'])(_node1)
        hashes = [
            _node1_module_dep('test').hash,
            _node1_function_dep1('test').hash,
            _node1_function_dep2('test').hash,
            _node1_string_dep('test').hash,
            _node1_bytes_dep('test').hash,
        ]
        # Check that all hashes are unique, except the last two which are equal
        self.assertEqual(len(set(hashes[:3])), 3)
        self.assertEqual(len(set(hashes[3:])), 1)

    def test_future_operator_access(self):
        # Test that MerkLFuture cannot be accessed by checking some operators
        future = embed_bert('sentence')
        with self.assertRaises(FutureAccessException):
            # Can't be added to set
            set([future])

        with self.assertRaises(FutureAccessException):
            # Can't be used as a truth value in statements
            if future:
                print('hello world')

        with self.assertRaises(FutureAccessException):
            # Can't be iterated over
            for x in future:
                print('hello world')

        with self.assertRaises(FutureAccessException):
            # Can't get a length from it
            print(len(future))

        with self.assertRaises(FutureAccessException):
            future += 1


if __name__ == '__main__':
    unittest.main()
