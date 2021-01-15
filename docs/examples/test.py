from merkl import task

@task
def task1(input_value):
    return 2 * input_value

@task
def task2(input_value):
    return input_value ** 2

def my_pipeline(input_value: int):
    val = task1(input_value)
    final_val = task2(val)
    return final_val
