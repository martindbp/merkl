from merkl import task

@task
def task1(input_value):
    return 2 * input_value

@task
def task2(input_value):
    return input_value ** 2

val = task1(3)
print(val)
print(val.eval())

final_val = task2(val)
print(final_val)
print(final_val.eval())
