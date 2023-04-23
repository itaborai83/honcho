import time
from datetime import datetime, timedelta

from honcho.models import *
from honcho.repository import *
from honcho.services import *
from honcho.exceptions import *
import honcho.util as util

class BaseTimeService:

    def now():
        raise NotImplementedError
        
    def sleep(self, seconds):
        raise NotImplementedError
    
    def subtract(self, dt1, dt2):
        dt1_parsed = util.parse_datetime(dt1)
        dt2_parsed = util.parse_datetime(dt2)
        delta = dt1_parsed - dt2_parsed
        return int(delta.total_seconds())

class TimeService(BaseTimeService):
    
    def now():
        return util.now()
    
    def sleep(self, seconds):
        time.sleep(seconds)
        
class MockTimeService(BaseTimeService):
    
    INITIAL_DATETIME = datetime(2000, 1, 1, 0, 0, 0)
    TIME_STEP = timedelta(0, 1)
    
    def __init__(self, initial=None):
        super().__init__()
        if initial is None:
            initial = self.INITIAL_DATETIME
        self.current = initial
    
    def now(self):
        return util.unparse_datetime(self.current)
    
    def sleep(self, seconds):
        assert seconds >= 0
        self.current = self.current + timedelta(0, seconds)
        
    def advance(self, seconds=None):
        self.current = self.current + self.TIME_STEP

class WorkerService:
    
    def __init__(self, collection, time_service):
        self.time_service = time_service
        self.collection = collection
    
    def create_worker(self, name):
        worker = Worker(name=name)
        worker.created_at = self.time_service.now()
        worker.updated_at = worker.created_at
        self.collection.insert(worker)
        return worker.dict()
    
    def get_worker(self, id):
        worker = self.collection.get(id)
        if worker is None:
            msg = f"worker #{id} not found"
            raise WorkerNotFoundError(msg)
        return worker.dict() if worker else None
        
    def delete_worker(self, id):
        worker = self.collection.get(id)
        if worker is None:
            msg = f"worker #{id} not found"
            raise WorkerNotFoundError(msg)
        if worker.status == WorkerStatus.BUSY:
            msg = f"worker #{id} is busy and cannot be deleted"
            raise WorkerIsBusyError(msg)
        was_deleted = self.collection.delete(id)
        assert was_deleted
        
    def list_workers(self, filter=None, reverse=False):
        workers = self.collection.list(filter=filter, reverse=reverse)
        return list([ w.dict() for w in workers ])
    
class WorkItemService:
    
    READY_COUNTER       = "READY_COUNTER"
    CHECKED_OUT_COUNTER = "CHECKED_OUT_COUNTER"
    FINISHED_COUNTER    = "FINISHED_COUNTER"
    ERROR_COUNTER       = "ERROR_COUNTER"
    TOTAL_COUNTER       = "TOTAL_COUNTER"
    
    def __init__(self, collection, finished_collection, error_collection, worker_service, counter_mngr, time_service):
        self.collection = collection
        self.finished_collection = finished_collection 
        self.error_collection = error_collection
        self.worker_service = worker_service
        self.counter_mngr = counter_mngr
        self.time_service = time_service
    
    def ensure_counters_exist(self):
        self.counter_mngr.ensure_counters_exist([
            self.READY_COUNTER
        ,   self.CHECKED_OUT_COUNTER
        ,   self.FINISHED_COUNTER
        ,   self.ERROR_COUNTER
        ,   self.TOTAL_COUNTER
        ])
        
    def create_work_item(self, name, payload):
        work_item = WorkItem(name=name, payload=payload)
        work_item.created_at = self.time_service.now()
        work_item.updated_at = work_item.created_at
        self.collection.insert(work_item)
        self.counter_mngr.increase(self.READY_COUNTER)
        self.counter_mngr.increase(self.TOTAL_COUNTER)
        return work_item.dict()
    
    def get_work_item(self, id):
        work_item = self.collection.get(id)
        if work_item is None:
            msg = f"work item #{id} not found"
            raise WorkItemNotFoundError(msg)
        return work_item.dict() if work_item else None
    
    def get_errored_work_item(self, id):
        work_item = self.error_collection.get(id)
        if work_item is None:
            msg = f"work item #{id} not found"
            raise WorkItemNotFoundError(msg)
        return work_item.dict() if work_item else None
        
    def delete_work_item(self, id):
        work_item = self.collection.get(id)
        if work_item is None:
            msg = f"work item #{id} not found"
            raise WorkItemNotFoundError(msg)
        if work_item.status == WorkItemStatus.CHECKED_OUT:
            msg = f"work item #{id} is checked out and cannot be deleted"
            raise WorkItemIsCheckedOutError(msg)
        was_deleted = self.collection.delete(id)
        assert was_deleted
        self.counter_mngr.decrease(self.READY_COUNTER)
        self.counter_mngr.decrease(self.TOTAL_COUNTER)
        
    def assign_work(self, worker_id):
        worker = self.worker_service.collection.get(worker_id)
        if worker.status == WorkerStatus.BUSY:
            msg = f"worker #{id} is busy"
            raise WorkerIsBusyError(msg)
        
        work_items = self.collection.list(
            filter  = lambda x: x.status == WorkItemStatus.READY
        ,   top_n   = 1
        ,   reverse = False
        )
        if len(work_items) == 0:
            raise NoWorkItemsLeftError("there are no ready work items left")
        
        work_item = work_items[0]
        
        worker.curr_work_item_id = work_item.id
        worker.status = WorkerStatus.BUSY
        worker.updated_at = self.time_service.now()
        work_item.status = WorkItemStatus.CHECKED_OUT
        work_item.updated_at = self.time_service.now()
        
        self.collection.update(work_item)
        self.worker_service.collection.update(worker)
        self.counter_mngr.decrease(self.READY_COUNTER)
        self.counter_mngr.increase(self.CHECKED_OUT_COUNTER)
        
    def finish_work(self, worker_id):
        worker = self.worker_service.collection.get(worker_id)
        if worker.status == WorkerStatus.IDLE:
            msg = f"worker #{id} is idle"
            raise WorkerIdleError(msg)
        
        work_item = self.collection.get(worker.curr_work_item_id)
        if work_item is None:
            msg = f"work item #{id} not found"
            raise WorkItemNotFoundError(msg)
        
        worker.worked_on.append(worker.curr_work_item_id)
        worker.curr_work_item_id = None
        worker.status = WorkerStatus.IDLE
        worker.updated_at = self.time_service.now()
        work_item.status = WorkItemStatus.FINISHED
        work_item.updated_at = self.time_service.now()
        
        self.collection.delete(work_item.id)
        self.finished_collection.put(work_item.id, work_item)
        self.worker_service.collection.update(worker)
        
        self.counter_mngr.decrease(self.CHECKED_OUT_COUNTER)
        self.counter_mngr.increase(self.FINISHED_COUNTER)
    
    def mark_error(self, worker_id, error):
        assert error
        worker = self.worker_service.collection.get(worker_id)
        if worker.status == WorkerStatus.IDLE:
            msg = f"worker #{id} is idle"
            raise WorkerIdleError(msg)
        
        work_item = self.collection.get(worker.curr_work_item_id)
        if work_item is None:
            msg = f"work item #{id} not found"
            raise WorkItemNotFoundError(msg)
        
        worker.worked_on.append(worker.curr_work_item_id)
        worker.curr_work_item_id = None
        worker.status = WorkerStatus.IDLE
        worker.updated_at = self.time_service.now()
        work_item.status = WorkItemStatus.ERROR
        work_item.updated_at = self.time_service.now()
        work_item.error = error
        
        self.collection.delete(work_item.id)
        self.error_collection.put(work_item.id, work_item)
        self.worker_service.collection.update(worker)
        
        self.counter_mngr.decrease(self.CHECKED_OUT_COUNTER)
        self.counter_mngr.increase(self.ERROR_COUNTER)
    
    def retry_work_item(self, max_items=100, max_retries=10):
        def filter(work_item):
            return work_item.retry_count < max_retries
        work_items = self.error_collection.list(top_n=max_items, filter=filter)
        for work_item in work_items:
            assert work_item.status == WorkItemStatus.ERROR
            work_item.status = WorkItemStatus.READY
            work_item.updated_at = self.time_service.now()
            work_item.retry_count += 1
            self.error_collection.delete(work_item.id)
            self.collection.update(work_item)
            self.counter_mngr.decrease(self.ERROR_COUNTER)
            self.counter_mngr.increase(self.READY_COUNTER)
            
    
    def time_out_workers(self, time_out):
        def filter(worker):
            return worker.status == WorkerStatus.BUSY
        workers_data = self.worker_service.list_workers(filter=filter)
        now = self.time_service.now()
        timed_out = []
        for worker_data in workers_data:
            total_time = self.time_service.subtract(now, worker_data['updated_at'])
            if total_time < time_out:
                continue
            worker_id = worker_data["id"]
            work_item_id = worker_data["curr_work_item_id"]
            error = f"worker #{worker_id} has been timed out while working on on work item #{work_item_id}"
            self.mark_error(worker_id, error)
            timed_out.append({
                "worker_id": worker_id
            ,   "work_item_id": work_item_id
            ,   "time_out": total_time
            })
        return timed_out
    
    def get_counters(self):
        ready           = self.counter_mngr.value(self.READY_COUNTER)
        checked_out     = self.counter_mngr.value(self.CHECKED_OUT_COUNTER)
        finished        = self.counter_mngr.value(self.FINISHED_COUNTER)
        error           = self.counter_mngr.value(self.ERROR_COUNTER)
        total           = self.counter_mngr.value(self.TOTAL_COUNTER)
        assert ready + checked_out + finished + error == total
        return {
            'datetime'   : self.time_service.now()
        ,   'ready'      : ready
        ,   'checked_out': checked_out
        ,   'finished'   : finished
        ,   'error'      : error
        ,   'total'      : total
        }