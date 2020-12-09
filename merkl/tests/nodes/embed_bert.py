from merkl import node
from time import sleep
import numpy as np


@node(outs=1)
def embed_bert(sentence):
    #sleep(1)
    return np.random.rand(700)

@node(outs=1)
def embed_bert_large(sentence):
    #sleep(1)
    return np.random.rand(700)
