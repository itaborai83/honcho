import unittest
import shutil
import lmdb

from honcho.models import *
from honcho.repository import *

class CommonCollectionTests:
    
    def test_it_encodes_keys(self):
        key_buf = self.collection._encode_key('test-key')
        self.assertEqual(key_buf, b'TEST:test-key')
    
    def test_it_decodes_keys_within_the_collection(self):
        prefix, key = self.collection._decode_key(b'TEST:test-key')
        self.assertEqual(prefix, b'TEST')
        self.assertEqual(key, 'test-key')

    def test_it_decodes_keys_outside_the_collection(self):
        prefix, key = self.collection._decode_key(b'XXXX:test-key')
        self.assertEqual(prefix, b'XXXX')
        self.assertEqual(key, None)
        
    def test_it_puts_a_value(self):
       self.collection.put('test-key', {'test': 'value'})
       value = self.collection.get('test-key')
       self.assertEqual(value, {'test': 'value'})
   
    def test_it_deletes_a_value(self):
       self.collection.put('test-key', {'test': 'value'})
       was_deleted = self.collection.delete('test-key')
       value = self.collection.get('test-key')
       self.assertTrue(was_deleted)
       self.assertEqual(value, None)
   
    def test_it_lists_values(self):
        self.collection.put('test-key-a', 'value-a')
        self.collection.put('test-key-c', 'value-c')
        self.collection.put('test-key-e', 'value-e')
        self.collection.put('test-key-d', 'value-d')
        self.collection.put('test-key-b', 'value-b')
        objs = self.collection.list()
        self.assertEqual(objs, ['value-a', 'value-b', 'value-c', 'value-d', 'value-e'])
        
    def test_it_lists_values_in_reverse_order(self):
        self.collection.put('test-key-a', 'value-a')
        self.collection.put('test-key-c', 'value-c')
        self.collection.put('test-key-e', 'value-e')
        self.collection.put('test-key-d', 'value-d')
        self.collection.put('test-key-b', 'value-b')
        objs = self.collection.list(reverse=True)
        objs.reverse()
        self.assertEqual(objs, ['value-a', 'value-b', 'value-c', 'value-d', 'value-e'])

    def test_it_lists_top_n_values(self):
        self.collection.put('test-key-a', 'value-a')
        self.collection.put('test-key-c', 'value-c')
        self.collection.put('test-key-e', 'value-e')
        self.collection.put('test-key-d', 'value-d')
        self.collection.put('test-key-b', 'value-b')
        objs = self.collection.list(top_n=3)
        self.assertEqual(objs, ['value-a', 'value-b', 'value-c'])

    def test_it_lists_top_n_values_in_reverse_order(self):
        self.collection.put('test-key-a', 'value-a')
        self.collection.put('test-key-c', 'value-c')
        self.collection.put('test-key-e', 'value-e')
        self.collection.put('test-key-d', 'value-d')
        self.collection.put('test-key-b', 'value-b')
        objs = self.collection.list(top_n=3, reverse=True)
        self.assertEqual(objs, ['value-e', 'value-d', 'value-c'])
    
    def test_it_lists_filtered_values(self):
        def filter(value):
            return value[-1] in ('a', 'c', 'e')
        
        self.collection.put('test-key-a', 'value-a')
        self.collection.put('test-key-c', 'value-c')
        self.collection.put('test-key-e', 'value-e')
        self.collection.put('test-key-d', 'value-d')
        self.collection.put('test-key-b', 'value-b')
        
        objs = self.collection.list(filter=filter)
        self.assertEqual(objs, ['value-a', 'value-c', 'value-e'])

class LmdbCollectionTest(unittest.TestCase, CommonCollectionTests):

    REPO_PATH = 'DATA/unittest-repo.lmdb'
    MAP_SIZE            = 1024 * 1024
    MAX_SPARE_TXNS      = 1000
    
    def setUp(self):
        self.env = lmdb.open(
            self.REPO_PATH
        ,   map_size        = self.MAP_SIZE
        ,   max_spare_txns  = self.MAX_SPARE_TXNS
        )
        self.env.reader_check()
        self.collection = LmdbCollection('TEST')
        self.collection.txn = self.env.begin(write=True)
        
    def tearDown(self):
        self.collection.txn.abort()
        self.env.close()
        shutil.rmtree(self.REPO_PATH, ignore_errors=False)

class MockCollectionTest(unittest.TestCase, CommonCollectionTests):
    
    def setUp(self):
        self.collection = MockCollection('TEST')
        
    def tearDown(self):
        pass
        
class CommonSequenceManagerTests:

    def test_sequence_creation(self):
        with self.assertRaises(AssertionError):
            self.sequence_mngr.currval('test-seqn-1')
        with self.assertRaises(AssertionError):
            self.sequence_mngr.currval('test-seqn-2')
        
        self.sequence_mngr.ensure_sequences_exist(['test-seqn-1', 'test-seqn-2'])
        
        self.assertEqual(self.sequence_mngr.currval('test-seqn-1'), 0)
        self.assertEqual(self.sequence_mngr.currval('test-seqn-2'), 0)
        
        self.assertEqual(self.sequence_mngr.nextval('test-seqn-1'), 1)
        self.assertEqual(self.sequence_mngr.nextval('test-seqn-2'), 1)
        
        self.assertEqual(self.sequence_mngr.currval('test-seqn-1'), 1)
        self.assertEqual(self.sequence_mngr.currval('test-seqn-2'), 1)
        
        self.sequence_mngr.ensure_sequences_exist(['test-seqn-1', 'test-seqn-2'])

        self.assertEqual(self.sequence_mngr.nextval('test-seqn-1'), 2)
        self.assertEqual(self.sequence_mngr.nextval('test-seqn-2'), 2)
        self.assertEqual(self.sequence_mngr.nextval('test-seqn-2'), 3)
        self.assertEqual(self.sequence_mngr.nextval('test-seqn-1'), 3)

class TestLmdbSequenceManager(unittest.TestCase, CommonSequenceManagerTests):

    REPO_PATH      = 'DATA/unittest-repo.lmdb'
    MAP_SIZE       = 1024 * 1024
    MAX_SPARE_TXNS = 1000
    
    def setUp(self):
        self.env = lmdb.open(
            self.REPO_PATH
        ,   map_size        = self.MAP_SIZE
        ,   max_spare_txns  = self.MAX_SPARE_TXNS
        )
        self.env.reader_check()
        self.sequence_mngr = LmdbSequenceManager()
        self.sequence_mngr.txn = self.env.begin(write=True)
        
    def tearDown(self):
        self.sequence_mngr.txn.abort()
        self.env.close()
        shutil.rmtree(self.REPO_PATH, ignore_errors=False)

class TestMockSequenceManager(unittest.TestCase, CommonSequenceManagerTests):
    
    def setUp(self):
        self.sequence_mngr = MockSequenceManager()
        
    def tearDown(self):
        self.sequence_mngr = None

class CommoWorkItemCollectionTests:
    
    def test_it_inserts_an_work_item(self):
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'})
        self.autoincr_collection.insert(work_item)
        self.assertEqual(work_item.id, 1)
    
    def test_it_deletes_an_work_item(self):
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'})
        self.autoincr_collection.insert(work_item)
        is_deleted = self.autoincr_collection.delete(work_item.id)
        self.assertTrue(is_deleted)
    
    def test_it_gets_an_work_item(self):
        now = util.now()
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'}, created_at=now, updated_at=now)
        self.autoincr_collection.insert(work_item)
        same_work_item = self.autoincr_collection.get(work_item.id)
        self.assertEqual(work_item, same_work_item)
    
    def test_it_updates_an_work_item(self):
        now = util.now()
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'}, created_at=now, updated_at=now)
        self.autoincr_collection.insert(work_item)
        work_item = self.autoincr_collection.get(work_item.id)
        work_item.updated_at = datetime(9999, 12, 31, 0, 0, 0)
        work_item.status = WorkItemStatus.ERROR
        self.autoincr_collection.update(work_item)
        same_work_item = self.autoincr_collection.get(1)
        self.assertEqual(work_item, same_work_item)
    
    def test_it_lists_work_items(self):
        now = util.now()
        work_item1 = WorkItem(name="Work Item 1", payload={'foo': 'bar 1'}, created_at=now, updated_at=now)
        work_item2 = WorkItem(name="Work Item 2", payload={'foo': 'bar 2'}, created_at=now, updated_at=now)
        work_item3 = WorkItem(name="Work Item 3", payload={'foo': 'bar 3'}, created_at=now, updated_at=now)
        work_item4 = WorkItem(name="Work Item 4", payload={'foo': 'bar 4'}, created_at=now, updated_at=now)
        
        self.autoincr_collection.insert(work_item1)
        self.autoincr_collection.insert(work_item2)
        self.autoincr_collection.insert(work_item3)
        self.autoincr_collection.insert(work_item4)
        work_items = self.autoincr_collection.list(top_n=3)
        self.assertEqual([work_item1, work_item2, work_item3], work_items)
    
    
    def test_it_deletes_a_work_item_mantaining_ordering(self):
        now = util.now()
        work_item1 = WorkItem(name="Work Item 1", payload={'foo': 'bar 1'}, created_at=now, updated_at=now)
        work_item2 = WorkItem(name="Work Item 2", payload={'foo': 'bar 2'}, created_at=now, updated_at=now)
        work_item3 = WorkItem(name="Work Item 3", payload={'foo': 'bar 3'}, created_at=now, updated_at=now)
        work_item4 = WorkItem(name="Work Item 4", payload={'foo': 'bar 4'}, created_at=now, updated_at=now)
        self.autoincr_collection.insert(work_item1)
        self.autoincr_collection.insert(work_item2)
        self.autoincr_collection.insert(work_item3)
        self.autoincr_collection.insert(work_item4)
        
        was_deleted = self.autoincr_collection.delete(work_item3.id)
        self.assertTrue(was_deleted)
        
        work_items = self.autoincr_collection.list(top_n=3)
        self.assertEqual([work_item1, work_item2, work_item4], work_items)
        
class LmdbWorkItemCollectionTests(unittest.TestCase, CommoWorkItemCollectionTests):
    
    REPO_PATH = 'DATA/unittest-repo.lmdb'
    MAP_SIZE            = 1024 * 1024
    MAX_SPARE_TXNS      = 1000
    
    def setUp(self):
        self.env = lmdb.open(
            self.REPO_PATH
        ,   map_size        = self.MAP_SIZE
        ,   max_spare_txns  = self.MAX_SPARE_TXNS
        )
        self.env.reader_check()
        self.collection = LmdbCollection('TEST', int_key=True)
        self.sequence_mngr = LmdbSequenceManager()
        self.sequence_name = 'SQ_WORK_ITEM'
        self.autoincr_collection = AutoIncrementCollection(self.collection, self.sequence_mngr, self.sequence_name)
        self.autoincr_collection.txn = self.env.begin(write=True)
        self.sequence_mngr.ensure_sequences_exist([self.sequence_name])
        
    def tearDown(self):
        self.collection.txn.abort()
        self.env.close()
        shutil.rmtree(self.REPO_PATH, ignore_errors=False)

    
class MockWorkItemCollectionTests(unittest.TestCase, CommoWorkItemCollectionTests):
    
    def setUp(self):
        self.collection = MockCollection('TEST', int_key=True)
        self.sequence_mngr = MockSequenceManager()
        self.sequence_name = 'SQ_WORK_ITEM'
        self.autoincr_collection = AutoIncrementCollection(self.collection, self.sequence_mngr, self.sequence_name)
        self.sequence_mngr.ensure_sequences_exist([self.sequence_name])
        
    def tearDown(self):
        pass




class CommoWorkerCollectionTests:
    
    def test_it_inserts_an_worker(self):
        worker = Worker(name="Test Worker 1")
        self.autoincr_collection.insert(worker)
        self.assertEqual(worker.id, 1)
        
    def test_it_deletes_an_worker(self):
        worker = Worker(name="Worker 1")
        self.autoincr_collection.insert(worker)
        is_deleted = self.autoincr_collection.delete(worker.id)
        self.assertTrue(is_deleted)
    
    def test_it_gets_an_worker(self):
        now = util.now()
        worker = Worker(name="Worker 1", created_at=now, updated_at=now)
        self.autoincr_collection.insert(worker)
        same_worker = self.autoincr_collection.get(worker.id)
        self.assertEqual(worker, same_worker)
    
    def test_it_updates_an_worker(self):
        now = util.now()
        worker = Worker(name="Work Item 1", created_at=now, updated_at=now)
        self.autoincr_collection.insert(worker)
        worker = self.autoincr_collection.get(worker.id)
        worker.updated_at = datetime(9999, 12, 31, 0, 0, 0)
        worker.status = WorkerStatus.BUSY
        self.autoincr_collection.update(worker)
        same_worker = self.autoincr_collection.get(1)
        self.assertEqual(worker, same_worker)
    
    def test_it_lists_workers(self):
        now = util.now()
        worker1 = Worker(name="Worker 1", created_at=now, updated_at=now)
        worker2 = Worker(name="Worker 2", created_at=now, updated_at=now)
        worker3 = Worker(name="Worker 3", created_at=now, updated_at=now)
        worker4 = Worker(name="Worker 4", created_at=now, updated_at=now)
        
        self.autoincr_collection.insert(worker1)
        self.autoincr_collection.insert(worker2)
        self.autoincr_collection.insert(worker3)
        self.autoincr_collection.insert(worker4)
        workers = self.autoincr_collection.list(top_n=3)
        self.assertEqual([worker1, worker2, worker3], workers)
    
    def test_it_deletes_a_worker_mantaining_ordering(self):
        now = util.now()
        worker1 = Worker(name="Worker 1", created_at=now, updated_at=now)
        worker2 = Worker(name="Worker 2", created_at=now, updated_at=now)
        worker3 = Worker(name="Worker 3", created_at=now, updated_at=now)
        worker4 = Worker(name="Worker 4", created_at=now, updated_at=now)
        
        self.autoincr_collection.insert(worker1)
        self.autoincr_collection.insert(worker2)
        self.autoincr_collection.insert(worker3)
        self.autoincr_collection.insert(worker4)
        
        was_deleted = self.autoincr_collection.delete(worker3.id)
        self.assertTrue(was_deleted)
        
        workers = self.autoincr_collection.list(top_n=3)
        self.assertEqual([worker1, worker2, worker4], workers)

class LmdbWorkerCollectionTests(unittest.TestCase, CommoWorkerCollectionTests):
    
    REPO_PATH = 'DATA/unittest-repo.lmdb'
    MAP_SIZE            = 1024 * 1024
    MAX_SPARE_TXNS      = 1000
    
    def setUp(self):
        self.env = lmdb.open(
            self.REPO_PATH
        ,   map_size        = self.MAP_SIZE
        ,   max_spare_txns  = self.MAX_SPARE_TXNS
        )
        self.env.reader_check()
        self.collection = LmdbCollection('TEST', int_key=True)
        self.sequence_mngr = LmdbSequenceManager()
        self.sequence_name = 'SQ_WORKER'
        self.autoincr_collection = AutoIncrementCollection(self.collection, self.sequence_mngr, self.sequence_name)
        self.autoincr_collection.txn = self.env.begin(write=True)
        self.sequence_mngr.ensure_sequences_exist([self.sequence_name])
        
    def tearDown(self):
        self.collection.txn.abort()
        self.env.close()
        shutil.rmtree(self.REPO_PATH, ignore_errors=False)

    
class MockWorkerCollectionTests(unittest.TestCase, CommoWorkerCollectionTests):
    
    def setUp(self):
        self.collection = MockCollection('TEST', int_key=True)
        self.sequence_mngr = MockSequenceManager()
        self.sequence_name = 'SQ_WORKER'
        self.autoincr_collection = AutoIncrementCollection(self.collection, self.sequence_mngr, self.sequence_name)
        self.sequence_mngr.ensure_sequences_exist([self.sequence_name])
        
    def tearDown(self):
        pass
