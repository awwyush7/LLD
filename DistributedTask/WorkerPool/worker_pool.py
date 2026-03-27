from DistributedTask.Task.task import Task
from threading import Semaphore, Thread
from DistributedTask.TaskQueue.task_queue import TaskQueue

class WorkerPool:
    def __init__(self, max_client, task_queue):
        self.max_client = max_client
        self.task_queue : TaskQueue = task_queue
        self.start()
    
    def start(self):
        threads = [Thread(target=self.do_task, daemon=False) for i in range(self.max_client)]
        for t in threads:
            t.start()
            
        for t in threads:
            t.join()    

    def do_actual_task(self, actual_task : callable):
        actual_task()

    def do_task(self):
        try:
            task : Task = self.task_queue.get_task()
            print(f"Trying to do {task.get_id()}")
            self.do_actual_task(task.get_task())
            task.complete()
        except Exception as e:
            # return {
            #     "status" : "Unabale to do task"
            # }
            raise e


