from merkl import task

@task
def embed_bert(sentence):
    return [1, 2]


@task
def embed_bert_large(sentence):
    return [1, 2, 3]
