from merkl import task

@task(outs=lambda data, k: k)
def split(data, k):
    ksize = len(data) // k
    return [data[i*ksize:(i+1)*ksize] for i in range(k)]

def pipeline():
    return split([1, 2, 3, 4, 5, 6], k=2)
