from queue import PriorityQueue
from DistributedTask.Task.task import Task

class TaskQueue:
    def __init__(self):
        self.queue = PriorityQueue(maxsize=10)

    def add_task(self, task : Task):
        try:
            self.queue.put(task, timeout=1)
        except Exception as e:
            raise e
    def get_task(self):
        try:
            task = self.queue.get(timeout=1)
            return task
        except Exception as e:
            raise e
    def queue_ended(self):
        return self.queue.empty()
    
    def size(self):
        return self.queue.qsize()
