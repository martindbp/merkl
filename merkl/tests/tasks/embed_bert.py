from merkl import task
from time import sleep
import numpy as np


@task(outs=1)
def embed_bert(sentence):
    #sleep(1)
    return 1, 2
    #return np.random.rand(700)

@task(outs=1)
def embed_bert_large(sentence):
    #sleep(1)
    return np.random.rand(700)
