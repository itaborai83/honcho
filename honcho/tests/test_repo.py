import unittest
import shutil


from honcho.models import *
from honcho.repository import *

class TestRepository(unittest.TestCase):
    REPO_PATH = 'DATA/unittest-repo.lmdb'
    
    def setUp(self):
        self.storage = Storage(self.REPO_PATH)
    
    def tearDown(self):
        self.storage.close()
        shutil.rmtree(self.REPO_PATH, ignore_errors=False)
    
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
    
    def test_it_deletes_a_work_item(self):
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'})
        self.storage.insert_work_item(work_item)
        is_deleted = self.storage.delete_work_item(work_item.id)
        self.assertTrue(is_deleted)
        
    def test_it_get_a_work_item(self):
        now = datetime.now()
        work_item = WorkItem(name="Work Item 1", payload={'foo': 'bar'}, created_at=now, updated_at=now)
        self.storage.insert_work_item(work_item)
        same_work_item = self.storage.get_work_item(work_item.id)
        self.assertEqual(work_item.id          , same_work_item.id         )
        self.assertEqual(work_item.name        , same_work_item.name       )
        self.assertEqual(work_item.payload     , same_work_item.payload    )
        self.assertEqual(work_item.error       , same_work_item.error      )
        self.assertEqual(work_item.retry_count , same_work_item.retry_count)
        self.assertEqual(work_item.status      , same_work_item.status     )
        self.assertEqual(work_item.created_at  , same_work_item.created_at )
        self.assertEqual(work_item.updated_at  , same_work_item.updated_at )
        
if __name__ == '__main__':
    unittest.main()