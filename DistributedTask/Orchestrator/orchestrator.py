from queue import Queue
from typing import List
from DistributedTask.TaskQueue.task_queue import TaskQueue
from DistributedTask.Task.task import Task
from DistributedTask.Task.task_priority import TaskPriority
from uuid import uuid4

class Orchestrator:

    def __init__(self):
        self.queue = TaskQueue()
        self.finished_tasks = set()
        self.depends = {}
        self.number_dependencies = {}

    def add_task(self, task: Task):
        # Only add to execution queue if dependencies are 0
        if self.number_dependencies[task] == 0:
            self.queue.add_task(task)
        
        for dependency in task.get_prerequisite():
            # task depends on dependency
            self.depends[dependency].append(task)
            self.number_dependencies[task] += 1
        
    def prerequisite_done(self,task : Task):
        for t in task.get_prerequisite():
            if t.get_id() not in self.finished_tasks:
                return False
        return True
    
    def complete(self, task : Task):
        for dependent in self.depends[task]:
            self.number_dependencies[dependent] = self.number_dependencies[dependent] - 1
            if(self.number_dependencies[dependent] == 0):
                self.add_task(dependent)
            

    def make_task(self, actual_task: callable, priority: TaskPriority, prerequisites: List[Task]):
        new_id = uuid4()
        task = Task(new_id, priority, prerequisites, actual_task, self.complete)
        
        # Initialize tracking for the new task
        self.depends[task] = []
        self.number_dependencies[task] = 0
        
        return task
    
    def queue_ended(self):
        return self.queue.queue_ended()