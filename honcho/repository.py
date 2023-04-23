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
    
    def __init__(self, coll_prefix, int_key=False):
        self.coll_prefix = coll_prefix.encode()
        self.int_key = int_key
        assert len(self.coll_prefix) == self.COLL_PREFIX_SIZE
                
    def _encode_key(self, key, int_key=None):
        if int_key is None:
            int_key = self.int_key
        if int_key:
            assert 0 <= key <= sys.maxsize
            return self.coll_prefix + self.PREFIX_SEPARATOR + key.to_bytes(16, 'big', signed=False)
        return self.coll_prefix + self.PREFIX_SEPARATOR + key.encode()
    
    def _decode_key(self, buf, int_key=None):
        if int_key is None:
            int_key = self.int_key        
        # extract the collection prefix from the key buffer
        prefix = buf[0:self.COLL_PREFIX_SIZE]
        # check if the prefix matches the prefix collection
        if prefix != self.coll_prefix:
            # when there is a mismatch, there is no pointing in trying to parse key
            # because the object does not belong to this collection
            return prefix, None
        # extract the key itself
        buf = buf[self.COLL_PREFIX_SIZE + 1:]
        if int_key:
            key = int.from_bytes(buf, 'big', signed=False)
            assert 0 <= key <= sys.maxsize
        else:
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
    
    def __init__(self, coll_prefix, int_key=False):
        super().__init__(coll_prefix, int_key)
        self.txn = None

    def _update_keyrange_for_put(self, key_buf):
        # can run after insertion took place
        
        # retrieve the key range
        range_key_buf = self._encode_key(self.KEYRANGE, int_key=False)
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
        range_key_buf = self._encode_key(self.KEYRANGE, int_key=False)
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
        range_key_buf = self._encode_key(self.KEYRANGE, int_key=False)
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
    
    def __init__(self, coll_prefix, int_key=False):
        super().__init__(coll_prefix, int_key)
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
        
    def run(self, include_keys=False):
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
            if include_keys:
                self.results.append((key, value))
            else:
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
        assert isinstance(sequence_names, list)
        for sequence_name in sequence_names:
            value = self.collection.get(sequence_name)
            if value is None:
                self.collection.put(sequence_name, 0)
        
    def nextval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value is not None and value >= 0, f"sequence '{sequence_name}' does not exist"
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
        assert isinstance(sequence_names, list)
        for sequence_name in sequence_names:
            value = self.collection.get(sequence_name)
            if value is None:
                self.collection[sequence_name] = 0
        
    def nextval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value is not None and value >= 0, f"sequence '{sequence_name}' does not exist"
        self.collection[sequence_name] = value+1
        return value+1
        
    def currval(self, sequence_name):
        value = self.collection.get(sequence_name)
        assert value is not None and value >= 0
        return value

class BaseCounterManager:

    def ensure_counters_exist(self, counter_names):
        raise NotImplementedError
        
    def increase(self, counter_name, ammount=1):
        raise NotImplementedError
        
    def decrease(self, counter_name, ammount=1):
        raise NotImplementedError
    
    def value(self, counter_name):
        raise NotImplementedError
        
        
class LmdbCounterManager(BaseCounterManager):
    
    def __init__(self, coll_prefix='CNTR'):
        super().__init__()
        self.collection = LmdbCollection(coll_prefix)
    
    @property
    def txn(self):
        return self.collection.txn
    
    @txn.setter
    def txn(self, txn):
        self.collection.txn = txn
    
    def ensure_counters_exist(self, counter_names):
        assert isinstance(counter_names, list)
        for counter_name in counter_names:
            value = self.collection.get(counter_name)
            if value is None:
                self.collection.put(counter_name, 0)

    def increase(self, counter_name, ammount=1):
        assert ammount > 0
        value = self.collection.get(counter_name)
        assert value is not None and value >= 0, f"counter '{counter_name}' does not exist"
        self.collection.put(counter_name, value + ammount)
        return value + ammount
    
    def decrease(self, counter_name, ammount=1):
        assert ammount > 0
        value = self.collection.get(counter_name)
        assert value is not None, f"counter '{counter_name}' does not exist"
        assert value > 0, f"can not decrease counter '{counter_name}'"
        self.collection.put(counter_name, value - ammount)
        return value - ammount

    def value(self, counter_name):
        value = self.collection.get(counter_name)
        assert value is not None and value >= 0
        return value

class MockCounterManager(BaseCounterManager):
    
    def __init__(self):
        super().__init__()
        self.collection = {}
        
    def ensure_counters_exist(self, counter_names):
        assert isinstance(counter_names, list)
        for counter_name in counter_names:
            value = self.collection.get(counter_name)
            if value is None:
                self.collection[counter_name] = 0
        
    def increase(self, counter_name, ammount=1):
        assert ammount > 0
        value = self.collection.get(counter_name)
        assert value is not None and value >= 0, f"counter '{counter_name}' does not exist"
        self.collection[counter_name] = value + ammount
        return value + ammount
        
    def decrease(self, counter_name, ammount=1):
        assert ammount > 0
        value = self.collection.get(counter_name)
        assert value is not None, f"counter '{counter_name}' does not exist"
        assert value > 0, f"can not decrease counter '{counter_name}'"
        self.collection[counter_name] = value - ammount
        return value - ammount
        
    def value(self, counter_name):
        value = self.collection.get(counter_name)
        assert value is not None and value >= 0
        return value
        
class AutoIncrementCollection:

    def __init__(self, collection, sequence_mngr, sequence_name):
        assert collection.int_key
        self.collection = collection
        self.sequence_mngr = sequence_mngr
        self.sequence_name = sequence_name
    
    @property
    def txn(self):
        return self.collection.txn
    
    @txn.setter
    def txn(self, txn):
        self.sequence_mngr.txn = txn
        self.collection.txn = txn
        
    def insert(self, obj):
        assert obj.id is None or obj.id == 0
        obj.id = self.sequence_mngr.nextval(self.sequence_name)
        self.collection.put(obj.id, obj)
    
    def update(self, obj):
        assert obj.id is not None and obj.id > 0
        self.collection.put(obj.id, obj)
    
    def get(self, id):
        assert id is not None and id > 0
        return self.collection.get(id)

    def delete(self, id):
        assert id is not None and id > 0
        return self.collection.delete(id)
        
    def list(self, filter=None, top_n=sys.maxsize, reverse=False):
        return self.collection.list(
            filter  = filter
        ,   top_n   = top_n
        ,   reverse = reverse
        )