import ray
import time

ray.init(address="ray://localhost:10001")

@ray.remote
def slow_double(x):
    time.sleep(1)
    return x * 2

results = ray.get([slow_double.remote(i) for i in range(5)])
print("Results:", results)
