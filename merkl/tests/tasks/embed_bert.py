from merkl import task

@task
def embed_bert(sentence):
    return [sentence[0], sentence[-1]]


@task
def embed_bert_large(sentence):
    return [sentence[0], sentence[-1], len(sentence)]
