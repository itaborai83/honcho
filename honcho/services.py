from datetime import datetime, timedelta

from honcho.models import *
from honcho.repository import *
from honcho.services import *
from honcho.exceptions import *
import honcho.util as util

class TimeService:
    
    def now():
        return util.now()

class MockTimeService:
    
    INITIAL_DATETIME = datetime(2000, 1, 1, 0, 0, 0)
    TIME_STEP = timedelta(0, 1)
    
    def __init__(self, initial=None):
        if initial is None:
            initial = self.INITIAL_DATETIME
        self.current = initial
    
    def now(self):
        return util.unparse_datetime(self.current)
    
    def advance(self):
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
        
    def list_workers(self, reverse=False):
        workers = self.collection.list(reverse=reverse)
        return list([ w.dict() for w in workers ])
    
class WorkItemService:
    
    def __init__(self, collection, finished_collection, error_collection, worker_service, time_service):
        self.collection = collection
        self.finished_collection = finished_collection 
        self.error_collection = error_collection
        self.worker_service = worker_service
        self.time_service = time_service
    
    def create_work_item(self, name, payload):
        work_item = WorkItem(name=name, payload=payload)
        work_item.created_at = self.time_service.now()
        work_item.updated_at = work_item.created_at
        self.collection.insert(work_item)
        return work_item.dict()
    
    def get_work_item(self, id):
        work_item = self.collection.get(id)
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
    
    def retry_work_item(self, max_items=100, max_retries=10):
        def filter(work_item):
            return work_item.retry_count < max_retries
        work_items = self.error_collection.list(top_n=max_items, filter=filter)
        for work_item in work_items:
            assert work_item.status == WorkItemStatus.ERROR
            work_item.status = WorkItemStatus.READY
            work_item.updated_at = self.time_service.now()
            work_item.retry_count += 1
            self.collection.update(work_item)
        