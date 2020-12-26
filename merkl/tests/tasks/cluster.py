from merkl import task
import numpy as np


@task(outs=2)
def cluster(asd, *embedded_sentences, k=3):
    indices = list(range(len(embedded_sentences)))
    mid = len(indices)//2
    clusters = [indices[:mid], indices[mid:]]
    num_clusters_out = 2
    return clusters, num_clusters_out
