from threading import Thread
from DistributedTask.Orchestrator.orchestrator import Orchestrator
from DistributedTask.WorkerPool.worker_pool import WorkerPool
from DistributedTask.Task.task import Task
from DistributedTask.Task.task_priority import TaskPriority

def first():
    print("I am the first func")

def second():
    print("I am the second func")

def third():
    print("I am the third func")


orches = Orchestrator()

task1 = orches.make_task(first, TaskPriority.MEDIUM, [])
task3 = orches.make_task(third, TaskPriority.HIGH, [])
task2 = orches.make_task(second, TaskPriority.LOW, [task3])


status1 = orches.add_task(task1)
# print(status1["status"])
orches.add_task(task3)
orches.add_task(task2)


def start_workers():
    workers = WorkerPool(2)

worker_starter_thread = Thread(target=start_workers, daemon=False)
worker_starter_thread.start()
worker_starter_thread.join()

