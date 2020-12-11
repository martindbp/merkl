import sys
import unittest
from io import StringIO

from nodes.embed_bert import embed_bert, embed_bert_large
from nodes.embed_elmo import embed_elmo
from nodes.cluster import cluster
from merkl import MerklFuture, node


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


class TestMerkl(unittest.TestCase):

    def test_node_hashing(self):
        # Test that hash is the same every time
        self.assertEqual(embed_bert('sentence').hash, embed_bert('sentence').hash)

        # Test that hash is different for different inputs
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert('another sentence').hash)

        # Test that hash is different with different function 
        self.assertNotEqual(embed_elmo('sentence').hash, embed_bert('sentence').hash)

        # Test that hash is different with different function in same file
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert_large('sentence').hash)

    def test_outs(self):
        @node
        def _node1(input_value):
            return input_value, 3

        out = _node1('test')
        # Single output by default
        self.assertTrue(isinstance(out, MerklFuture))

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

    def test_future_operator_access(self):
        # Test that MerklFuture cannot be accessed by checking some operators
        future = embed_bert('sentence')
        with self.assertRaises(MerklFuture.MerklFutureAccessException):
            # Can't be added to set
            set([future])

        with self.assertRaises(MerklFuture.MerklFutureAccessException):
            # Can't be used as a truth value in statements
            if future:
                print('hello world')

        with self.assertRaises(MerklFuture.MerklFutureAccessException):
            # Can't be iterated over
            for x in future:
                print('hello world')

        with self.assertRaises(MerklFuture.MerklFutureAccessException):
            # Can't get a length from it
            print(len(future))

        with self.assertRaises(MerklFuture.MerklFutureAccessException):
            future += 1



if __name__ == '__main__':
    unittest.main()
