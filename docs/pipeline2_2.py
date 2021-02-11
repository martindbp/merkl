from merkl import task, read_future, write_future, pipeline


@task
def train(data, iterations):
    return 3


@task
def evaluate(model, data):
    return data[:model] 


@pipeline
def train_eval():
    train_data = read_future('train.csv')
    test_data = read_future('test.csv')
    model = train(train_data, iterations=100)
    score = evaluate(model, test_data)
    return score, write_future(model, 'model.csv')
