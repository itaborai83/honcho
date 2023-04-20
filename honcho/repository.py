import lmdb
import threading
import json
from typing import Optional, List, Set, Dict
from honcho.models import WorkItem, WorkItemStatus, Worker, WorkerStatus
from honcho.exceptions import *
import honcho.util as util

class BaseStorage:
    
    ###########################################################################
    ## Sequence related methods
    ###########################################################################
    def nextval(self, sequence_name):
        raise NotImplementedError
        
    def currval(self, sequence_name):
        raise NotImplementedError   
                    
    ###########################################################################
    ## Work Item Methods
    ###########################################################################
    def _parse_work_item(self, buf):
        raise NotImplementedError
    
    def _unparse_work_item(self, work_item):
        raise NotImplementedError
        
    def insert_work_item(self, work_item: WorkItem) -> None:
        if work_item.id != 0:
            msg = f"work item '{work_item.name}' is not transient and cannot be created"
            raise WorkItemNotTransientError(msg)
        work_item.id = self.nextval(self.WORK_ITEM_SEQUENCE)
        self._insert_work_item(work_item)

    def _insert_work_item(self, work_item: WorkItem) -> None:
        raise NotImplementedError
    
    def get_work_item(self, work_item_id: int) -> Optional[WorkItem]:
        if work_item_id == 0:
            msg = f"work item id #{work_item_id}' is invalid"
            raise InvalidWorkItemIdError(msg)
        return self._get_work_item(work_item_id)
        
    def _get_work_item(self, work_item_id: int) -> Optional[WorkItem]:
        raise NotImplementedError
            
    def delete_work_item(self, work_item_id: int) -> bool:
        if work_item_id == 0:
            msg = f"work item id #{work_item_id}' is invalid"
            raise InvalidWorkItemIdError(msg)
        return self._delete_work_item(work_item_id)
        
    def _delete_work_item(self, work_item_id: int) -> bool:
        raise NotImplementedError
        
    def update_work_item(self, work_item: WorkItem) -> None:
        if work_item.id == 0:
            msg = f"work item '{work_item.name}' is transient and can not be updated"
            raise TransientWorkItemError(msg)
        self._update_work_item(work_item)
    
    def _update_work_item(self, work_item: WorkItem) -> None:
        raise NotImplementedError
        
    def list_work_items(self, status: WorkItemStatus, first_n=100) -> List[WorkItem]:
        result = []
        count = 0
        for work_item in self._iter_work_items():
            if work_item.status != status:
                continue
            if count >= first_n:
                break
            result.append(work_item)
            count += 1
        return result
        
    def _iter_work_items(self):
        raise NotImplementedError
        
    ###########################################################################
    ## Worker Methods
    ###########################################################################
    def _parse_worker(self, buf):
        raise NotImplementedError
    
    def _unparse_worker(self, worker):
        raise NotImplementedError
        
    def insert_worker(self, worker: Worker) -> None:
        if worker.id != 0:
            msg = f"worker '{worker.name}' is not transient and cannot be created"
            raise WorkerNotTransientError(msg)
        worker.id = self.nextval(self.WORKER_SEQUENCE)
        self._insert_worker(worker)

    def _insert_worker(self, worker: Worker) -> None:
        raise NotImplementedError
    
    def get_worker(self, worker_id: int) -> Optional[Worker]:
        if worker_id == 0:
            msg = f"worker id #{worker_id}' is invalid"
            raise InvalidWorkerIdError(msg)
        return self._get_worker(worker_id)
        
    def _get_worker(self, worker_id: int) -> Optional[Worker]:
        raise NotImplementedError
    
    def delete_worker(self, worker_id: int) -> bool:
        if worker_id == 0:
            msg = f"worker id #{worker_id}' is invalid"
            raise InvalidWorkerIdError(msg)
        return self._delete_worker(worker_id)
        
    def _delete_worker(self, worker_id: int) -> bool:
        raise NotImplementedError
        
    def update_worker(self, worker: Worker) -> None:
        if worker.id == 0:
            msg = f"worker '{worker.name}' is transient and can not be updated"
            raise TransientWorkerError(msg)
        self._update_worker(worker)
    
    def _update_worker(self, worker: Worker) -> None:
        raise NotImplementedError

    def list_workers(self, status: WorkerStatus, first_n=100) -> List[Worker]:
        result = []
        count = 0
        for worker in self._iter_workers():
            if worker.status != status:
                continue
            if count >= first_n:
                break
            result.append(worker)
            count += 1
        return result
        
    def _iter_workers(self):
        raise NotImplementedError

class Storage(BaseStorage):
    
    MAP_SIZE            = 1024 * 1024
    MAX_SPARE_TXNS      = 1000
    SEQUENCES_DB        = b'SEQUENCES_DB'
    WORK_ITEM_DB        = b'WORK_ITEM_DB'
    MAX_DBS             = 2
    WORKER_SEQUENCE     = 'SQ_WORKER'
    WORK_ITEM_SEQUENCE  = 'SQ_WORK_ITEM'
    EXPECTED_SEQUENCES  = [WORKER_SEQUENCE, WORK_ITEM_SEQUENCE]
    PREFIX_SIZE         = 4
    WORKER_PREFIX       = b'WRKR'
    WORK_ITEM_PREFIX    = b'WKIT'
    ERROR_PREFIX        = b'WIER'

    def __init__(self, db_path, map_size=MAP_SIZE):
        self.db_path = db_path
        self.db_lock = threading.RLock()
        with self.db_lock:
            self._init_db()

    def _init_db(self):
        self.env             = lmdb.open(self.db_path, map_size=self.MAP_SIZE, max_dbs=self.MAX_DBS, max_spare_txns=self.MAX_SPARE_TXNS)
        self.sequences_db    = self.env.open_db(self.SEQUENCES_DB)
        self.work_item_db    = self.env.open_db(self.WORK_ITEM_DB)
        self.env.reader_check()
        self.ensure_sequences_exist(self.EXPECTED_SEQUENCES)
    
    ###########################################################################
    ## General
    ###########################################################################
    def close(self):
        self.env.close()
    
    ###########################################################################
    ## Helper methods
    ###########################################################################
    def _int2key(self, prefix, i):
        return prefix + i.to_bytes(16, 'big', signed=False)
    
    def _key2int(self, buf):
        prefix = buf[0:self.PREFIX_SIZE]
        buf = buf[self.PREFIX_SIZE:]
        i = int.from_bytes(buf, 'big', signed=False)
        return prefix, i

    ###########################################################################
    ## Sequence related methods
    ###########################################################################
    def ensure_sequences_exist(self, sequence_names):
        with self.db_lock,\
             self.env.begin(write=True, db=self.sequences_db) as txn:
            for sequence_name in sequence_names:
                key = sequence_name.encode()
                if txn.get(key) is None:
                    value = util.json2buffer(0)
                    txn.put(key, value)
    
    def nextval(self, sequence_name):
        with self.db_lock,\
             self.env.begin(write=True, db=self.sequences_db) as txn:
            return self._nextval(txn, sequence_name)

    def currval(self, sequence_name):
        with self.db_lock,\
             self.env.begin(write=True, db=self.sequences_db) as txn:
            return self._currval(txn, sequence_name)
    
    def _nextval(self, txn, sequence_name):
        key = sequence_name.encode()
        curval = self._currval(txn, sequence_name)
        value = util.json2buffer(curval + 1)
        txn.put(key, value)
        return curval + 1
        
    def _currval(self, txn, sequence_name):
        key = sequence_name.encode()
        buf = txn.get(key, None)
        assert buf is not None
        id = util.buffer2json(buf)
        assert isinstance(id, int)
        return id
        
    ###########################################################################
    ## Work Item Methods
    ###########################################################################
    def _parse_work_item(self, buf):
        if isinstance(buf, bytes):
            buf = buf.decode()
        return WorkItem.from_json(buf)
    
    def _unparse_work_item(self, work_item):
        return work_item.json().encode() if work_item else None
        
    def _insert_work_item(self, work_item: WorkItem) -> None:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = self._unparse_work_item(work_item)
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item.id)
            txn.put(key, buf)
    
    def _get_work_item(self, work_item_id: int) -> Optional[WorkItem]:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item_id)
            buf = txn.get(key, None)
            return self._parse_work_item(buf)
    
    def _delete_work_item(self, work_item_id: int) -> bool:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item_id)
            is_deleted = txn.delete(key)
            return is_deleted
    
    def _update_work_item(self, work_item: WorkItem) -> None:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = self._unparse_work_item(work_item)
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item.id)
            txn.put(key, buf)
   
    def _iter_work_items(self):
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            cursor = txn.cursor()
            count = 0
            for key, buf in cursor:
                if not key.startswith(self.WORK_ITEM_PREFIX):
                    return
                work_item = self._parse_work_item(buf)
                yield work_item
        
    ###########################################################################
    ## Worker Methods
    ###########################################################################
    def _parse_worker(self, buf):
        if isinstance(buf, bytes):
            buf = buf.decode()
        return Worker.from_json(buf)
    
    def _unparse_worker(self, worker):
        return worker.json().encode() if worker else None
        
    def _insert_worker(self, worker: Worker) -> None:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = self._unparse_worker(worker)
            key = self._int2key(self.WORKER_PREFIX, worker.id)
            txn.put(key, buf)
            
    def _get_worker(self, worker_id: int) -> Optional[Worker]:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORKER_PREFIX, worker_id)
            buf = txn.get(key, None)
            return self._parse_worker(buf)
    
    def _delete_worker(self, worker_id: int) -> bool:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORKER_PREFIX, worker_id)
            is_deleted = txn.delete(key)
            return is_deleted
        
    def _update_worker(self, worker: Worker) -> None:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = self._unparse_worker(worker)
            key = self._int2key(self.WORKER_PREFIX, worker.id)
            txn.put(key, buf)

    def _iter_workers(self):
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            cursor = txn.cursor()
            count = 0
            for key, buf in cursor:
                if not key.startswith(self.WORKER_PREFIX):
                    return
                worker = self._parse_worker(buf)
                yield worker

class MockStorage(BaseStorage):

    WORKER_SEQUENCE     = 'SQ_WORKER'
    WORK_ITEM_SEQUENCE  = 'SQ_WORK_ITEM'
    EXPECTED_SEQUENCES  = [WORKER_SEQUENCE, WORK_ITEM_SEQUENCE]
    
    def __init__(self):
        self.sequences = { self.WORKER_SEQUENCE: 0, self.WORK_ITEM_SEQUENCE: 0 }
        self.workers = {}
        self.work_items = {}
    
    ###########################################################################
    ## General
    ###########################################################################
    def close(self):
        pass
        
    ###########################################################################
    ## Sequence related methods
    ###########################################################################
        
    def nextval(self, sequence_name):
        self.sequences[sequence_name] += 1
        return self.sequences[sequence_name]

    def currval(self, sequence_name):
        return self.sequences[sequence_name]
                
    ###########################################################################
    ## Work Item Methods
    ###########################################################################
    def _parse_work_item(self, buf):
        if isinstance(buf, bytes):
            buf = buf.decode()
        return WorkItem.from_json(buf)
    
    def _unparse_work_item(self, work_item):
        return work_item.json().encode() if work_item else None
        
    def _insert_work_item(self, work_item: WorkItem) -> None:
        self.work_items[ work_item.id ] = self._unparse_work_item(work_item)
    
    def _delete_work_item(self, work_item_id: int) -> bool:
        is_present = work_item_id in self.work_items
        if is_present:
            del self.work_items[work_item_id]
            return True
        return False
    
    def _get_work_item(self, work_item_id: int) -> Optional[WorkItem]:
        buf = self.work_items.get(work_item_id)
        return WorkItem.from_json(buf)

    def _update_work_item(self, work_item: WorkItem) -> None:
        self.work_items[ work_item.id ] = self._unparse_work_item(work_item)
   
    def _iter_work_items(self):
        for buf in self.work_items.values():
            work_item = self._parse_work_item(buf)
            yield work_item
            
    ###########################################################################
    ## Worker Methods
    ###########################################################################
    def _parse_worker(self, buf):
        if isinstance(buf, bytes):
            buf = buf.decode()
        return Worker.from_json(buf)
    
    def _unparse_worker(self, worker):
        return worker.json().encode() if worker else None
        
    def _insert_worker(self, worker: Worker) -> None:
        self.workers[ worker.id ] = self._unparse_worker(worker)
    
    def _delete_worker(self, worker_id: int) -> bool:
        is_present = worker_id in self.workers
        if is_present:
            del self.workers[worker_id]
            return True
        return False
    
    def _get_worker(self, worker_id: int) -> Optional[Worker]:
        buf = self.workers.get(worker_id)
        return Worker.from_json(buf)

    def _update_worker(self, work_item: Worker) -> None:
        self.workers[ work_item.id ] = self._unparse_work_item(work_item)
   
    def _iter_workers(self):
        for buf in self.workers.values():
            work_item = self._parse_worker(buf)
            yield work_item