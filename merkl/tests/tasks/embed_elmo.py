from merkl import task
from time import sleep
import numpy as np


@task(outs=1)
def embed_elmo(sentence):
    #sleep(1)
    return np.random.rand(700)
