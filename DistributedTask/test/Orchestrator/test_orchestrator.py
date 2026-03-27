import pytest
from DistributedTask.Orchestrator.orchestrator import Orchestrator
from DistributedTask.WorkerPool.worker_pool import WorkerPool
from threading import Thread
from DistributedTask.Task.task import Task
from DistributedTask.Task.task_priority import TaskPriority
@pytest.fixture
def get_orchestrator():
    worker_pool_test = WorkerPool(3)
    return Orchestrator(worker_pool_test)

def test_add_max_tasks(get_orchestrator):
    def actual_test_task():
        pass
    def add_tasks():
        task = Task("123",TaskPriority.LOW,[], actual_test_task)
        get_orchestrator.add_task(task)

    threads = [Thread(target = add_tasks, args = (), daemon=False) for i in range(15)]
    for t in threads:
            t.start()

    for t in threads:
        t.join(timeout=1)

    assert get_orchestrator.queue.size() == 10

