import unittest
import shutil


from honcho.models import *
from honcho.repository import *

class CommonStorageTests:

    def test_it_retrieves_the_next_worker_id(self):
        nextval = self.storage.nextval('SQ_WORKER')
        currval = self.storage.currval('SQ_WORKER')
        nextval2 = self.storage.nextval('SQ_WORKER')
        
        self.assertEqual(nextval, 1)
        self.assertEqual(currval, 1)
        self.assertEqual(nextval2, 2)
        
    def test_it_retrieves_the_next_work_item_id(self):
        nextval = self.storage.nextval('SQ_WORK_ITEM')
        currval = self.storage.currval('SQ_WORK_ITEM')
        nextval2 = self.storage.nextval('SQ_WORK_ITEM')
        
        self.assertEqual(nextval, 1)
        self.assertEqual(currval, 1)
        self.assertEqual(nextval2, 2)
    
    def test_it_inserts_an_work_item(self):
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'})
        self.storage.insert_work_item(work_item)
        self.assertEqual(work_item.id, 1)

    def test_it_inserts_an_worker(self):
        worker = Worker(name="Test Worker 1")
        self.storage.insert_worker(worker)
        self.assertEqual(worker.id, 1)
    
    def test_it_deletes_an_work_item(self):
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'})
        self.storage.insert_work_item(work_item)
        is_deleted = self.storage.delete_work_item(work_item.id)
        self.assertTrue(is_deleted)

    def test_it_deletes_an_worker(self):
        worker = Worker(name="Worker 1")
        self.storage.insert_worker(worker)
        is_deleted = self.storage.delete_worker(worker.id)
        self.assertTrue(is_deleted)
        
    def test_it_gets_an_work_item(self):
        now = util.now()
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'}, created_at=now, updated_at=now)
        self.storage.insert_work_item(work_item)
        same_work_item = self.storage.get_work_item(work_item.id)
        self.assertEqual(work_item, same_work_item)

    def test_it_gets_an_worker(self):
        now = util.now()
        worker = Worker(name="Worker 1", created_at=now, updated_at=now)
        self.storage.insert_worker(worker)
        same_worker = self.storage.get_worker(worker.id)
        self.assertEqual(worker, same_worker)
    
    def test_it_updates_an_work_item(self):
        now = util.now()
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'}, created_at=now, updated_at=now)
        self.storage.insert_work_item(work_item)
        work_item = self.storage.get_work_item(work_item.id)
        work_item.updated_at = datetime(9999, 12, 31, 0, 0, 0)
        work_item.status = WorkItemStatus.ERROR
        self.storage.update_work_item(work_item)
        same_work_item = self.storage.get_work_item(1)
        self.assertEqual(work_item, same_work_item)

    def test_it_updates_an_worker(self):
        now = util.now()
        worker = Worker(name="Work Item 1", created_at=now, updated_at=now)
        self.storage.insert_worker(worker)
        worker = self.storage.get_worker(worker.id)
        worker.updated_at = datetime(9999, 12, 31, 0, 0, 0)
        worker.status = WorkerStatus.BUSY
        self.storage.update_worker(worker)
        same_worker = self.storage.get_worker(1)
        self.assertEqual(worker, same_worker)
    
    def test_it_lists_work_items(self):
        now = util.now()
        work_item1 = WorkItem(name="Work Item 1", payload={'foo': 'bar 1'}, created_at=now, updated_at=now)
        work_item2 = WorkItem(name="Work Item 2", payload={'foo': 'bar 2'}, created_at=now, updated_at=now)
        work_item3 = WorkItem(name="Work Item 3", payload={'foo': 'bar 3'}, created_at=now, updated_at=now)
        work_item4 = WorkItem(name="Work Item 4", payload={'foo': 'bar 4'}, created_at=now, updated_at=now)
        
        self.storage.insert_work_item(work_item1)
        self.storage.insert_work_item(work_item2)
        self.storage.insert_work_item(work_item3)
        self.storage.insert_work_item(work_item4)
        work_items = self.storage.list_work_items(WorkItemStatus.READY, first_n=3)
        self.assertEqual([work_item1, work_item2, work_item3], work_items)

    def test_it_lists_workers(self):
        now = util.now()
        worker1 = Worker(name="Worker 1", created_at=now, updated_at=now)
        worker2 = Worker(name="Worker 2", created_at=now, updated_at=now)
        worker3 = Worker(name="Worker 3", created_at=now, updated_at=now)
        worker4 = Worker(name="Worker 4", created_at=now, updated_at=now)
        
        self.storage.insert_worker(worker1)
        self.storage.insert_worker(worker2)
        self.storage.insert_worker(worker3)
        self.storage.insert_worker(worker4)
        workers = self.storage.list_workers(WorkerStatus.IDLE, first_n=3)
        self.assertEqual([worker1, worker2, worker3], workers)
    
    def test_it_deletes_a_work_item_mantaining_ordering(self):
        now = util.now()
        work_item1 = WorkItem(name="Work Item 1", payload={'foo': 'bar 1'}, created_at=now, updated_at=now)
        work_item2 = WorkItem(name="Work Item 2", payload={'foo': 'bar 2'}, created_at=now, updated_at=now)
        work_item3 = WorkItem(name="Work Item 3", payload={'foo': 'bar 3'}, created_at=now, updated_at=now)
        work_item4 = WorkItem(name="Work Item 4", payload={'foo': 'bar 4'}, created_at=now, updated_at=now)
        self.storage.insert_work_item(work_item1)
        self.storage.insert_work_item(work_item2)
        self.storage.insert_work_item(work_item3)
        self.storage.insert_work_item(work_item4)
        
        was_deleted = self.storage.delete_work_item(work_item3.id)
        self.assertTrue(was_deleted)
        
        work_items = self.storage.list_work_items(WorkItemStatus.READY, first_n=3)
        self.assertEqual([work_item1, work_item2, work_item4], work_items)

    def test_it_deletes_a_worker_mantaining_ordering(self):
        now = util.now()
        worker1 = Worker(name="Worker 1", created_at=now, updated_at=now)
        worker2 = Worker(name="Worker 2", created_at=now, updated_at=now)
        worker3 = Worker(name="Worker 3", created_at=now, updated_at=now)
        worker4 = Worker(name="Worker 4", created_at=now, updated_at=now)
        
        self.storage.insert_worker(worker1)
        self.storage.insert_worker(worker2)
        self.storage.insert_worker(worker3)
        self.storage.insert_worker(worker4)
        
        was_deleted = self.storage.delete_worker(worker3.id)
        self.assertTrue(was_deleted)
        
        workers = self.storage.list_workers(WorkerStatus.IDLE, first_n=3)
        self.assertEqual([worker1, worker2, worker4], workers)

class TestStorage(unittest.TestCase, CommonStorageTests):
    
    REPO_PATH = 'DATA/unittest-repo.lmdb'
    
    def setUp(self):
        self.storage = Storage(self.REPO_PATH)
        #self.maxDiff = None
        
    def tearDown(self):
        self.storage.close()
        shutil.rmtree(self.REPO_PATH, ignore_errors=False)
    
class TestMockStorage(unittest.TestCase, CommonStorageTests):
    
    def setUp(self):
        self.storage = MockStorage()
        
    def tearDown(self):
        self.storage.close()

if __name__ == '__main__':
    unittest.main()