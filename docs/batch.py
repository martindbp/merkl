from merkl import batch, task

@task
def embed_single_word(word):
    return f'some embedding of {word}'

@task(outs=lambda words: len(words))
def embed_words_as_task(words):
    return tuple([embed_single_word(word) for word in words])

@batch(embed_single_word)
def embed_words_batch(words):
    return tuple([embed_single_word(word) for word in words])

def pipeline1():
    return embed_words_as_task(['word', 'word', 'another'])

def pipeline2():
    return embed_words_batch(['word', 'word', 'another'])
