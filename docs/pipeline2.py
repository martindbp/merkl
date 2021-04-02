from merkl import task, read_future

@task
def train(data, iterations):
    return 'trained model'

@task
def evaluate(model, data):
    return 99.3

def train_eval():
    train_data = read_future('train.csv')
    test_data = read_future('test.csv')
    model = train(train_data, iterations=100)
    model > 'model.bin'
    score = evaluate(model, test_data)
    return score, model
