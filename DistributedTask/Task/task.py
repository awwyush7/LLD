from __future__ import annotations
from typing import List, Optional
from DistributedTask.Task.task_priority import TaskPriority

class Task:
    def __init__(self, id, priority : TaskPriority , prerequisite : Optional[List[Task]], actual_task : callable, complete : callable):
        self.__id = id
        self._priority = priority
        self._prerequisite = prerequisite
        self.__task = actual_task
        self.__complete = complete
        
    def __lt__(self, other):
        # 1. Compare by actual priority value
        if self.get_priority() != other.get_priority():
            return self.get_priority() > other.get_priority()
        
        # 2. Tie-breaker: Compare by ID to avoid infinite recursion
        return self.get_id() < other.get_id()
    
    def change_priority(self, new_priority : TaskPriority):
        self._priority = new_priority
        
    def get_priority(self):
        return self._priority.value
    
    def get_id(self):
        return self.__id
    
    def get_task(self):
        return self.__task
    
    def get_prerequisite(self):
        return self._prerequisite
    
    @property
    def complete(self):
        return self.__complete