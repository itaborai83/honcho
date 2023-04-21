import sys
import lmdb
import threading
import pickle
import json
from typing import Optional, List, Set, Dict
from honcho.models import WorkItem, WorkItemStatus, Worker, WorkerStatus
from honcho.exceptions import *
import honcho.util as util


class BaseCollection:
    
    PREFIX_SEPARATOR    = b':'
    COLL_PREFIX_SIZE    = 4
    
    def __init__(self, coll_prefix):
        self.coll_prefix = coll_prefix.encode()
        assert len(self.coll_prefix) == self.COLL_PREFIX_SIZE
                
    def _encode_key(self, key):
        return self.coll_prefix + self.PREFIX_SEPARATOR + key.encode()
    
    def _decode_key(self, buf):
        # extract the collection prefix from the key buffer
        prefix = buf[0:self.COLL_PREFIX_SIZE]
        # check if the prefix matches the prefix collection
        if prefix != self.coll_prefix:
            # when there is a mismatch, there is no pointing in trying to parse key
            # because the object does not belong to this collection
            return prefix, None
        # extract the key itself
        buf = buf[self.COLL_PREFIX_SIZE + 1:]
        key = buf.decode()
        return prefix, key
        
    def _encode_value(self, obj):
        return pickle.dumps(obj)
    
    def _decode_value(self, buf):
        if buf is None:
            return None
        return pickle.loads(buf)
    
    def put(self, key, value):
        raise NotImplementedError
    
    def get(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError
    
    def list(self, filter=None, top_n=sys.maxsize, reverse=False):
        raise NotImplementedError


class LmdbCollection(BaseCollection):
    
    KEYRANGE = '__keyrange__'
    
    def __init__(self, coll_prefix):
        super().__init__(coll_prefix)
        self.txn = None

    def _update_keyrange_for_put(self, key_buf):
        # can run after insertion took place
        
        # retrieve the key range
        range_key_buf = self._encode_key(self.KEYRANGE)
        value_buf = self.txn.get(range_key_buf)
        
        # handle first put
        if value_buf is None:
            # create the key range on the first put
            keyrange = (key_buf, key_buf)
            value_buf = self._encode_value(keyrange)
            self.txn.put(range_key_buf, value_buf)
            return
            
        # decode the existing key range
        keyrange = self._decode_value(value_buf)
        min_key_buf, max_key_buf = keyrange
        
        if key_buf < min_key_buf:
            min_key_buf = key_buf
            
        if key_buf > max_key_buf:
            max_key_buf = key_buf
        
        keyrange = (min_key_buf, max_key_buf)
        value_buf = self._encode_value(keyrange)
        self.txn.put(range_key_buf, value_buf)

    def _update_keyrange_for_deletion(self, key_buf):
        # needs to run before deletion takes place
        cursor = self.txn.cursor()
        # checks if value exists
        positioned = cursor.set_range(key_buf)
        if not positioned:
            return
        
        # retrieve the key range
        range_key_buf = self._encode_key(self.KEYRANGE)
        value_buf = cursor.get(range_key_buf)
        # there has to be a key range because the deleted key exists
        assert value_buf is not None 
        
        # decode the existing key range
        keyrange = self._decode_value(value_buf)
        min_key_buf, max_key_buf = keyrange
        
        # since the key exists, it needs to be greater than or equals to the min_key
        assert key_buf >= min_key_buf
        # since the key exists, it needs to be less than or equals to the max_key
        assert key_buf <= max_key_buf
            
        if min_key_buf == max_key_buf:
            # handle single key deletion
            was_deleted = cursor.delete(range_key_buf)
            assert was_deleted
            return
            
        elif key_buf == min_key_buf:
            # shrink left side of the range
            positioned = cursor.set_range(min_key_buf)
            assert positioned
            positioned = cursor.next()
            assert positioned
            min_key_buf = cursor.key()
            assert min_key_buf.startswith(self.coll_prefix)
        
        elif key_buf == max_key_buf:
            # shrink right side of the range
            positioned = cursor.set_range(max_key_buf)
            assert positioned
            positioned = cursor.prev()
            assert positioned
            max_key_buf = cursor.key()
            assert max_key_buf.startswith(self.coll_prefix)
        
        else:
            # key range not affected by the deletion of an intermediate value
            return
        
        cursor.close()
        keyrange = (min_key_buf, max_key_buf)
        value_buf = self._encode_value(keyrange)
        self.txn.put(value_buf)
        
    def put(self, key, value):
        buf_key = self._encode_key(key)
        buf_value = self._encode_value(value)
        self.txn.put(buf_key, buf_value)
        self._update_keyrange_for_put(buf_key)
    
    def get(self, key):
        assert key
        buf_key = self._encode_key(key)
        buf_value = self.txn.get(buf_key, None)
        value = self._decode_value(buf_value)
        return value

    def delete(self, key):
        assert key
        buf_key = self._encode_key(key)
        self._update_keyrange_for_deletion(buf_key)
        was_deleted = self.txn.delete(buf_key)
        return was_deleted
    
    def list(self, filter=None, top_n=sys.maxsize, reverse=False):
        
        # retrieve the key range
        range_key_buf = self._encode_key(self.KEYRANGE)
        value_buf = self.txn.get(range_key_buf)
        if value_buf is None:
            # no key range means no values present
            return []
        
        # decode the existing key range
        keyrange = self._decode_value(value_buf)
        min_key_buf, max_key_buf = keyrange
        
        it = LmdbCollectionIterator(
            txn             = self.txn
        ,   collection      = self
        ,   min_key_buf     = min_key_buf
        ,   max_key_buf     = max_key_buf
        ,   filter          = filter
        ,   top_n           = top_n
        ,   reverse         = reverse
        )
        return it.run()

class MockCollection(BaseCollection):
    
    def __init__(self, coll_prefix):
        super().__init__(coll_prefix)
        self.data = {}
        
    def put(self, key, value):
        key_buf = self._encode_key(key)
        value_buf = self._encode_value(value)
        self.data[key_buf] = value_buf
    
    def get(self, key):
        key_buf = self._encode_key(key)
        value_buf = self.data.get(key_buf)
        value = self._decode_value(value_buf)
        return value
    
    def delete(self, key):
        key_buf = self._encode_key(key)
        was_deleted = key_buf in self.data
        if was_deleted:
            del self.data[key_buf]
        return was_deleted
        
    def list(self, filter=None, top_n=sys.maxsize, reverse=False):
        if len(self.data) == 0:
            return []
        
        it = MockCollectionIterator(
            collection      = self
        ,   filter          = filter
        ,   top_n           = top_n
        ,   reverse         = reverse
        )
        return it.run()

class LmdbCollectionIterator:
    
    START_KEY   = b''
    END_KEY     = b'\xff\xff\xff\xff\xff\xff\xff\xff'
    
    def __init__(self, txn, collection, min_key_buf, max_key_buf, filter=None, top_n=sys.maxsize, reverse=False):
        self.txn            = txn
        self.cursor         = None
        self.collection     = collection
        self.start_key_buf  = min_key_buf
        self.end_key_buf    = max_key_buf
        self.filter         = filter
        self.top_n          = top_n
        self.reverse        = reverse
        self.results        = []
        self.count          = 0
        
        if self.reverse:
            self.start_key_buf, self.end_key_buf = self.end_key_buf, self.start_key_buf
    
    def start(self):
        self.results = []
        self.count   = 0
        self.cursor  = self.txn.cursor()
        positioned   = self.cursor.set_range(self.start_key_buf)
        return positioned
        
    def advance(self):
        if not self.reverse:
            positioned = self.cursor.next()
        else:
            positioned = self.cursor.prev()
        return positioned
        
    def is_past_end_key(self, key_buf):
        if not self.reverse:
            return key_buf > self.end_key_buf
        else:
            return key_buf < self.end_key_buf
    
    def filter_value(self, value):
        if self.filter is None:
            return True
        return self.filter(value)
    
    def update_count(self):
        self.count += 1
        count_reached = self.count >= self.top_n
        return count_reached
        
    def run(self):
        positioned = self.start()
        if not positioned:
            return self.results
        
        bail_out = False
        advance_cursor = False
        while True: 
            
            if bail_out:
                break
            
            if advance_cursor:
                positioned = self.advance()
                if not positioned:
                    bail_out = True
                    continue
            
            advance_cursor = True
            
            # are we past the end key?
            key_buf = self.cursor.key()
            if self.is_past_end_key(key_buf):
                bail_out = True
                continue
            
            # parse the object
            value_buf = self.cursor.value()
            value = self.collection._decode_value(value_buf)
            
            # apply filter
            if not self.filter_value(value):
                continue
            
            # add object to the result
            self.results.append(value)
            count_reached = self.update_count()
            if count_reached:
                bail_out = True
                continue
        
        return self.results

class MockCollectionIterator:
        
    def __init__(self, collection, filter=None, top_n=sys.maxsize, reverse=False):
        self.collection     = collection
        self.filter         = filter
        self.top_n          = top_n
        self.reverse        = reverse
        self.results        = []
        self.count          = 0
        self.idx            = 0 if not reverse else len(collection.data)-1
        self.keys           = None
        
    def start(self):
        if len(self.collection.data) == 0:
            positioned = False
            return positioned
        self.results = []
        self.keys    = list(sorted(self.collection.data.keys()))
        self.count   = 0
        positioned   = True
        return positioned
        
    def advance(self):
        if not self.reverse:
            self.idx += 1
            positioned = self.idx < len(self.collection.data)
        else:
            self.idx -= 1
            positioned = self.idx >= 0
        return positioned
    
    def filter_value(self, value):
        if self.filter is None:
            return True
        return self.filter(value)
    
    def update_count(self):
        self.count += 1
        count_reached = self.count >= self.top_n
        return count_reached
        
    def run(self):
        positioned = self.start()
        if not positioned:
            return self.results
        
        bail_out = False
        advance_cursor = False
        while True: 
            
            if bail_out:
                break
            
            if advance_cursor:
                positioned = self.advance()
                if not positioned:
                    bail_out = True
                    continue
            
            advance_cursor = True
            
            # parse the object
            key_buf = self.keys[self.idx]
            value_buf = self.collection.data[key_buf]
            value = self.collection._decode_value(value_buf)
            
            # apply filter
            if not self.filter_value(value):
                continue
            
            # add object to the result
            self.results.append(value)
            count_reached = self.update_count()
            if count_reached:
                bail_out = True
                continue
        
        return self.results


class BaseSequenceManager:
            
    def ensure_sequences_exist(self, sequence_names):
        raise NotImplementedError
        
    def nextval(self, sequence_name):
        raise NotImplementedError
        
    def currval(self, sequence_name):
        raise NotImplementedError

class LmdbSequenceManager(BaseSequenceManager):
    
    def __init__(self, coll_prefix='SEQN'):
        super().__init__()
        self.collection = LmdbCollection(coll_prefix)
    
    @property
    def txn(self):
        return self.collection.txn
    
    @txn.setter
    def txn(self, txn):
        self.collection.txn = txn
    
    def ensure_sequences_exist(self, sequence_names):
        for sequence_name in sequence_names:
            value = self.collection.get(sequence_name)
            if value is None:
                self.collection.put(sequence_name, 0)
        
    def nextval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value >= 0
        self.collection.put(sequence_name, value+1)
        return value+1
        
    def currval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value is not None and value >= 0
        return value
        
class MockSequenceManager(BaseSequenceManager):
    
    def __init__(self):
        super().__init__()
        self.collection = {}
        
    def ensure_sequences_exist(self, sequence_names):
        for sequence_name in sequence_names:
            value = self.collection.get(sequence_name)
            if value is None:
                self.collection[sequence_name] = 0
        
    def nextval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value >= 0
        self.collection[sequence_name] = value+1
        return value+1
        
    def currval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value is not None and value >= 0
        return value

"""
class LmdbBaseCollection:
    
    MAX_ID = sys.maxsize
    
    def __init__(self, coll_prefix):
        super(self).__init__(sequence_mngr, sequence_name)
        self.coll_prefix = coll_prefix.encode()
        self.prefix_size = len(self.coll_prefix)
        self.txn = None
        
    def _int2key(self, i):
        assert 0 <= i <= self.MAX_ID
        return self.coll_prefix + i.to_bytes(16, 'big', signed=False)
    
    def _key2int(self, buf):
        prefix = buf[0:self.prefix_size]
        assert prefix == self.coll_prefix
        buf = buf[self.prefix_size:]
        i = int.from_bytes(buf, 'big', signed=False)
        assert 0 <= i <= self.MAX_ID
        return prefix, i
        
    def _parse_object(self, buf):
        raise NotImplementedError
    
    def _unparse_object(self, obj):
        raise NotImplementedError
    
    def insert(self, obj):
        assert obj.id is None or obj.id == 0
        work_item.id = self.sequence_mngr.nextval()
        key = self._int2key(obj.id)
        buf = self._unparse_object(obj)
        self.txn.put(key, buf)
    
    def get(self, id):
        assert id != 0 and id is not None
        key = self._int2key(id)
        buf = self.txn.get(key, None)
        return self._parse_object(buf)

    def delete(self, id):
        assert id != 0 and id is not None
        key = self._int2key(id)
        is_deleted = self.txn.delete(key)
        return is_deleted

    def update(self, obj):
        assert obj.id != 0 and obj.id is not None
        key = self._int2key(obj.id)
        buf = self._unparse_object(obj)
        self.txn.put(key, buf)
    
    def list(self, filter=None, top_n=MAX_ID, start_key=0, end_key=MAX_ID):
        count = 0
        result = []
        _start_key = self._int2key(start_key)
        _end_key = self._int2key(end_key)
        
        cursor = txn.cursor()
        positioned = cursor.set_range(_start_key)
        if not positioned:
            return result
        
        bail_out = False
        advance_cursor = False
        while True:
            
            if bail_out:
                return result
            
            if advance_cursor:
                positioned = cursor.next()
                bail_out = not positioned
                if bail_out:
                    continue
            
            advance_cursor = True
            
            # are we within the bounds of the current collection?
            key = cursor.key()
            if key > _end_key:
                bail_out = True
                continue
            
            # parse the object
            buf = cursor.value()
            obj = self._parse_object(buf)
            
            # apply filter
            if filter and not filter(obj):
                continue
            
            # add object to the result
            result.append(obj)
            count += 1
            
            # check if we reached top_n
            if count >= top_n:
                bail_out = True
                continue
            
        return result







class LmdbSequenceManager:
    
    MAP_SIZE            = 1024 * 1024
    MAX_SPARE_TXNS      = 1000
    
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
    
    
    def __init__(self):
        self.txn = None
    
    def ensure_sequences_exist(self, sequence_names):
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

       
class BaseCollection:

    def __init__(self, sequence_mngr, sequence_name):
        self.sequence_mngr = sequence_mngr
        self.sequence_name = sequence_name
        
    def _parse_element(self, buf):
        raise NotImplementedError
    
    def _unparse_element(self, obj):
        raise NotImplementedError
    
    def insert(self, obj):
        raise NotImplementedError
    
    def get(self, id):
        raise NotImplementedError
        
    def delete(self, id):
        raise NotImplementedError
    
    def update(self, obj):
        raise NotImplementedError
    
    def list(self, filter=None, top_n=MAX_ID):
        raise NotImplementedError

class LmdbBaseCollection:
    
    MAX_ID = sys.maxsize
    
    def __init__(self, sequence_mngr, sequence_name, coll_prefix):
        super(self).__init__(sequence_mngr, sequence_name)
        self.coll_prefix = coll_prefix.encode()
        self.prefix_size = len(self.coll_prefix)
        self.txn = None
        
    def _int2key(self, i):
        assert 0 <= i <= self.MAX_ID
        return self.coll_prefix + i.to_bytes(16, 'big', signed=False)
    
    def _key2int(self, buf):
        prefix = buf[0:self.prefix_size]
        assert prefix == self.coll_prefix
        buf = buf[self.prefix_size:]
        i = int.from_bytes(buf, 'big', signed=False)
        assert 0 <= i <= self.MAX_ID
        return prefix, i
        
    def _parse_object(self, buf):
        raise NotImplementedError
    
    def _unparse_object(self, obj):
        raise NotImplementedError
    
    def insert(self, obj):
        assert obj.id is None or obj.id == 0
        work_item.id = self.sequence_mngr.nextval()
        key = self._int2key(obj.id)
        buf = self._unparse_object(obj)
        self.txn.put(key, buf)
    
    def get(self, id):
        assert id != 0 and id is not None
        key = self._int2key(id)
        buf = self.txn.get(key, None)
        return self._parse_object(buf)

    def delete(self, id):
        assert id != 0 and id is not None
        key = self._int2key(id)
        is_deleted = self.txn.delete(key)
        return is_deleted

    def update(self, obj):
        assert obj.id != 0 and obj.id is not None
        key = self._int2key(obj.id)
        buf = self._unparse_object(obj)
        self.txn.put(key, buf)
    
    def list(self, filter=None, top_n=MAX_ID, start_key=0, end_key=MAX_ID):
        count = 0
        result = []
        _start_key = self._int2key(start_key)
        _end_key = self._int2key(end_key)
        
        cursor = txn.cursor()
        positioned = cursor.set_range(_start_key)
        if not positioned:
            return result
        
        bail_out = False
        advance_cursor = False
        while True:
            
            if bail_out:
                return result
            
            if advance_cursor:
                positioned = cursor.next()
                bail_out = not positioned
                if bail_out:
                    continue
            
            advance_cursor = True
            
            # are we within the bounds of the current collection?
            key = cursor.key()
            if key > _end_key:
                bail_out = True
                continue
            
            # parse the object
            buf = cursor.value()
            obj = self._parse_object(buf)
            
            # apply filter
            if filter and not filter(obj):
                continue
            
            # add object to the result
            result.append(obj)
            count += 1
            
            # check if we reached top_n
            if count >= top_n:
                bail_out = True
                continue
            
        return result

class BaseStorage:
    
    WORKER_SEQUENCE     = 'SQ_WORKER'
    WORK_ITEM_SEQUENCE  = 'SQ_WORK_ITEM'
    EXPECTED_SEQUENCES  = [WORKER_SEQUENCE, WORK_ITEM_SEQUENCE]
    
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
"""