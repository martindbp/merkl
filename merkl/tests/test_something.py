import unittest

from nodes.embed_bert import embed_bert, embed_bert_large
from nodes.embed_elmo import embed_elmo
from nodes.cluster import cluster
#from pipelines.clustering_pipeline import clustering_pipeline

class TestSomething(unittest.TestCase):

    def test_nodes(self):
        # Check that hash is the same every time
        self.assertEqual(embed_bert('sentence').hash, embed_bert('sentence').hash)

        # Check that hash is different for different inputs
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert('another sentence').hash)

        # Check that hash is different with different function 
        self.assertNotEqual(embed_elmo('sentence').hash, embed_bert('sentence').hash)

        # Check that hash is different with different function in same file
        self.assertNotEqual(embed_bert('sentence').hash, embed_bert_large('sentence').hash)

if __name__ == '__main__':
    unittest.main()
