from merkl import task, mread


@task
def train(data, iterations):
    return 3


@task
def evaluate(model, data):
    return data[:model] 


def train_eval():
    train_data = mread('train.csv')
    test_data = mread('test.csv')
    model = train(train_data, iterations=100)
    score = evaluate(model, test_data)
    return model, score
