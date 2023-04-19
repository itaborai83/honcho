import lmdb
import threading
import json
from typing import Optional, List, Set, Dict
from honcho.models import WorkItem, WorkItemStatus, Worker, WorkerStatus
from honcho.exceptions import *
import honcho.util as util


class Storage:
    
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
                    self._create_sequence(txn, sequence_name)
        
    def create_sequence(self, sequence_name):
        with self.db_lock,\
             self.env.begin(write=True, db=self.sequences_db) as txn:
            return self._create_sequence(txn, sequence_name)

    def nextval(self, sequence_name):
        with self.db_lock,\
             self.env.begin(write=True, db=self.sequences_db) as txn:
            return self._nextval(txn, sequence_name)

    def currval(self, sequence_name):
        with self.db_lock,\
             self.env.begin(write=True, db=self.sequences_db) as txn:
            return self._currval(txn, sequence_name)

    def _ensure_sequence_does_not_exist(self, txn, sequence_name):
        key = sequence_name.encode()
        if txn.get(key) is not None:
            msg = f"sequence '{sequence_name}' already exists"
            raise SequenceAlreadyExistsError(msg)
    
    def _create_sequence(self, txn, sequence_name):
        self._ensure_sequence_does_not_exist(txn, sequence_name)
        key = sequence_name.encode()
        value = util.json2buffer(0)
        txn.put(key, value)
                
            
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
    def insert_work_item(self, work_item: WorkItem) -> None:
        if work_item.id != 0:
            msg = f"work item '{work_item.name}' is not transient and cannot be created"
            raise WorkItemNotTransientError(msg)
        work_item.id = self.nextval(self.WORK_ITEM_SEQUENCE)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = work_item.json().encode()
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item.id)
            txn.put(key, buf)
    
    def get_work_item(self, work_item_id: int) -> Optional[WorkItem]:
        if work_item_id == 0:
            msg = f"work item id #{work_item_id}' is invalid"
            raise InvalidWorkItemIdError(msg)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item_id)
            result = txn.get(key, None)
            if result:
                data = json.loads(result.decode())
                return WorkItem(**data)
            return None
            
    def delete_work_item(self, work_item_id: int) -> bool:
        if work_item_id == 0:
            msg = f"work item id #{work_item_id}' is invalid"
            raise InvalidWorkItemIdError(msg)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item_id)
            is_deleted = txn.delete(key)
            return is_deleted
    
    def update_work_item(self, work_item: WorkItem) -> None:
        if work_item.id == 0:
            msg = f"work item '{work_item.name}' is transient and can not be updated"
            raise TransientWorkItemError(msg)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = work_item.json().encode()
            key = self._int2key(self.WORK_ITEM_PREFIX, work_item.id)
            txn.put(key, buf)
    
    def list_work_items(self, status: WorkItemStatus, first_n=100) -> List[WorkItem]:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            result = []
            cursor = txn.cursor()
            count = 0
            for key, value in cursor:
                if not key.startswith(self.WORK_ITEM_PREFIX):
                    break
                data = json.loads(value.decode())
                work_item = WorkItem(**data)
                if work_item.status != status:
                    continue
                if count >= first_n:
                    break
                result.append(work_item)
                count += 1
            return result
            
    ###########################################################################
    ## Worker Methods
    ###########################################################################
    def insert_worker(self, worker: Worker) -> None:
        if worker.id != 0:
            msg = f"worker '{worker.name}' is not transient and cannot be created"
            raise WorkerNotTransientError(msg)
        worker.id = self.nextval(self.WORKER_SEQUENCE)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = worker.json().encode()
            key = self._int2key(self.WORKER_PREFIX, worker.id)
            txn.put(key, buf)

    def delete_worker(self, worker_id: int) -> bool:
        if worker_id == 0:
            msg = f"worker id #{worker_id}' is invalid"
            raise InvalidWorkerIdError(msg)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORKER_PREFIX, worker_id)
            is_deleted = txn.delete(key)
            return is_deleted
    
    def get_worker(self, worker_id: int) -> Optional[Worker]:
        if worker_id == 0:
            msg = f"worker id #{worker_id}' is invalid"
            raise InvalidWorkerIdError(msg)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            key = self._int2key(self.WORKER_PREFIX, worker_id)
            result = txn.get(key, None)
            if result:
                data = json.loads(result.decode())
                return Worker(**data)
            return None
        
    def update_worker(self, worker: Worker) -> None:
        if worker.id == 0:
            msg = f"worker '{worker.name}' is transient and can not be updated"
            raise TransientWorkerError(msg)
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            buf = worker.json().encode()
            key = self._int2key(self.WORKER_PREFIX, worker.id)
            txn.put(key, buf)

    def list_workers(self, status: WorkerStatus, first_n=100) -> List[Worker]:
        with self.db_lock,\
             self.env.begin(write=True, db=self.work_item_db) as txn:
            result = []
            cursor = txn.cursor()
            count = 0
            for key, value in cursor:
                if not key.startswith(self.WORKER_PREFIX):
                    break
                data = json.loads(value.decode())
                worker = Worker(**data)
                if worker.status != status:
                    continue
                if count >= first_n:
                    break
                result.append(worker)
                count += 1
            return result