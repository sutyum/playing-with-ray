import ray
import time

ray.init()  # Add `ignore_reinit_error=True`


# 1. Remote Task
@ray.remote
def add(a, b):
    return a + b


result_ref = add.remote(1, 2)
print(ray.get(result_ref))


@ray.remote
def slow_function(seconds):
    time.sleep(seconds)
    return seconds


# # 2. Parrallel Execution
# # Execute 5 tasks in parallel (~1 second)
# t0 = time.time()
# refs = [slow_function.remote(1) for _ in range(10)]
# result = ray.get(refs)
# t1 = time.time()
# print("Duration(s):", t1 - t0)


# 3. Actors
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def value(self):
        return self.value

    def increment(self):
        self.value += 1
        return self.value


counter = Counter.remote()

counter.increment.remote()
counter.increment.remote()

print(ray.get(counter.value.remote()))  # 2


# 4. Object References and Dependencies
@ray.remote
def multiply(a, b):
    return a * b


x_ref = add.remote(1, 2)
y_ref = multiply.remote(x_ref, 3)
print(ray.get(y_ref))  # Output: (1+2)*3 = 9


# 5. Task Dependencies
@ray.remote
def fail():
    raise ValueError("Oops!")


try:
    ray.get(fail.remote())
except ValueError as e:
    print("ValueError! :", e)
except ray.exceptions.RayTaskError as e:
    print("RayTaskError! :", e)
