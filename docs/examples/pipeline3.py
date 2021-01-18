from merkl import task, fread, fwrite


@task
def train(data, iterations):
    return 3


@task
def evaluate(model, data):
    return data[:model] 


def train_eval():
    train_data = fread('train.csv')
    test_data = fread('test.csv')
    model = train(train_data, iterations=100)
    score = evaluate(model, test_data)
    return score, fwrite(model, 'model.csv')
