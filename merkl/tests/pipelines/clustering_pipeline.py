from merkl.tests.tasks.embed_bert import embed_bert
from merkl.tests.tasks.cluster import cluster


def clustering_pipeline():
    sentences = ['sentence1', 'sentence2', 'sentence3']
    embedded_sentences = [embed_bert(s) for s in sentences]
    clusters = cluster(*embedded_sentences, k=2)
    #print(clusters.get())
    #print(clusters[0].hash)
    #print(clusters[1].hash)
    print(clusters[0].get())
    print(clusters[1].get())
    #breakpoint()
    return clusters


if __name__ == '__main__':
    clustering_pipeline()
