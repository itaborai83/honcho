from enum import Enum
from datetime import datetime, timedelta
from typing import List, Set, Dict, Optional, Any
from pydantic import BaseModel, Field, validator, ValidationError
from pydantic.json import timedelta_isoformat
import honcho.util as util

from honcho.exceptions import *

class WorkerStatus(int, Enum):
    IDLE    = 1
    BUSY    = 2

class AppState(BaseModel):
    current_worker_id       : int = 0
    current_work_item_id    : int = 0
    
    def next_worker_id(self) -> int:
        self.current_worker_id += 1
        return self.current_worker_id
    
    def next_work_item_id(self) -> int:
        self.current_work_item_id += 1
        return self.current_worker_id
        
class Worker(BaseModel):
    id                  : int = 0
    name                : str
    curr_work_item_id   : Optional[int]
    worked_on           : List[int] = Field(default_factory=list)
    status              : WorkerStatus = WorkerStatus.IDLE
    created_at          : Optional[datetime] = None
    updated_at          : Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: util.convert_datetime_to_iso_8601_with_z_suffix,
            timedelta: timedelta_isoformat,
        }

    def is_transient(self):
        return self.id == 0
    
    def ensure_non_transient(self):
        if self.is_transient():
            msg = f"worker '{self.name}' is transient"
            raise TransientWorkerError(msg)
    
    def is_idle(self):
        return self.status == WorkerStatus.IDLE
    
    def ensure_idle(self):
        if not self.is_idle():
            msg = f"worker '{self.name}' is busy working on work item #{self.curr_work_item_id}"
            raise WorkerBusyError(msg)

    def is_busy(self):
        return self.status == WorkerStatus.BUSY
        
    def ensure_working_on(self, work_item):
        if not self.is_busy():
            msg = f"worker '{self.name}' is idle"
            raise WorkerIdleError(msg)
        if self.curr_work_item_id != work_item.id:
            msg = f"worker '{self.name}' not working on work item #{work_item.id}"
            raise WrongWorkItemError(msg)
    
    def work_on(self, work_item: "WorkItem") -> None:
        self.ensure_non_transient()
        self.ensure_idle()
        work_item.ensure_non_transient()
        work_item.ensure_ready()
        
        self.curr_work_item_id = work_item.id
        self.worked_on.append(work_item.id)
        self.status = WorkerStatus.BUSY
        self.updated_at = datetime.now()
        
        work_item.status = WorkItemStatus.CHECKED_OUT
        work_item.updated_at = datetime.now()
        
    def finish_work_on(self, work_item: "WorkItem") -> None:
        work_item.ensure_non_transient()
        work_item.ensure_checked_out()
        self.ensure_non_transient()
        self.ensure_working_on(work_item)
        
        self.curr_work_item_id = None
        self.status = WorkerStatus.IDLE
        self.updated_at = datetime.now()
        
        work_item.status = WorkItemStatus.FINISHED
        work_item.updated_at = datetime.now()
        
    def error_working_on(self, work_item: "WorkItem", error: str) -> None:
        work_item.ensure_non_transient()
        work_item.ensure_checked_out()
        self.ensure_non_transient()
        self.ensure_working_on(work_item)
        
        self.curr_work_item_id = None
        self.status = WorkerStatus.IDLE
        self.updated_at = datetime.now()
        
        work_item.status = WorkItemStatus.ERROR
        work_item.updated_at = datetime.now()
        work_item.error = error
        
class WorkItemStatus(int, Enum):
    READY       = 1
    CHECKED_OUT = 2
    FINISHED    = 3
    ERROR       = 4
    
class WorkItem(BaseModel):
    id          : int = 0
    name        : str
    payload     : Optional[Dict[str, Any]]
    error       : Optional[str]
    retry_count : int = 0
    status      : WorkItemStatus = WorkItemStatus.READY
    created_at  : Optional[datetime] = None
    updated_at  : Optional[datetime] = None
    
    class Config:
        json_encoders = {
            datetime: util.convert_datetime_to_iso_8601_with_z_suffix,
            timedelta: timedelta_isoformat,
        }
        
    def is_transient(self):
        return self.id == 0
    
    def ensure_non_transient(self):
        if self.is_transient():
            msg = f"worker '{self.name}' is transient"
            raise TransientWorkItemError(msg)
    
    def is_ready(self):
        return self.status == WorkItemStatus.READY
    
    def ensure_ready(self):
        if not self.is_ready():
            msg = f"work item '{self.name}' is not ready"
            raise WorkItemNotReadyError(msg)

    def ensure_not_ready(self):
        if not not self.is_ready():
            msg = f"work item '{self.name}' should not be ready"
            raise WorkItemShouldNotBeReadyError(msg)
    
    def is_checked_out(self):
        return self.status == WorkItemStatus.CHECKED_OUT
    
    def ensure_checked_out(self):
        if not self.is_checked_out():
            msg = f"work item '{self.name}' is not ready"
            raise WorkItemNotReadyError(msg)

    def is_finished(self):
        return self.status == WorkItemStatus.FINISHED

    def ensure_not_finished(self):
        if not not self.is_finished():
            msg = f"work item '{self.name}' should not be finished"
            raise WorkItemShouldNotBeFinishedError(msg)
    
    def reset(self):
        self.ensure_non_transient()
        self.ensure_not_ready()
        self.ensure_not_finished()
        self.error       = None
        self.retry_count += 1
        self.status      = WorkItemStatus.READY
        self.updated_at  = datetime.now()
        