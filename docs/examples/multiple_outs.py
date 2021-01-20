from merkl import task

@task
def task1():
    return 1, 2, 3

@task
def task2(input1, input2, input3):
    return {'out1': 1, 'out2': 2, 'out3': 3}

def my_pipeline():
    outs = task1()
    outs = task2(*outs)
    return outs
