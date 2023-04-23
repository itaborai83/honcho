import unittest

from honcho.models import *
from honcho.repository import *
from honcho.services import *
from honcho.exceptions import *

import honcho.util as util

from pprint import pprint


class WorkerServiceTest(unittest.TestCase):

    def setUp(self):    
        self.sequence_mngr = MockSequenceManager()
        self.sequence_name = 'SQ_WORKER'
        self.collection = AutoIncrementCollection(
            MockCollection('WRKR', int_key=True)
        ,   self.sequence_mngr
        ,   self.sequence_name
        )
        self.sequence_mngr.ensure_sequences_exist([self.sequence_name])
        self.service = WorkerService(
            self.collection
        ,   MockTimeService()
        )
        
    def tearDown(self):
        pass
            
    def test_it_creates_a_worker(self):
        worker_data = self.service.create_worker('test-worker')
        same_worker_data = self.service.get_worker(1)
        
        self.assertEqual(worker_data, same_worker_data)
        self.assertEqual(worker_data['id'], 1)
        self.assertEqual(worker_data['name'], 'test-worker')
        self.assertEqual(worker_data['status'], WorkerStatus.IDLE)
        self.assertEqual(worker_data['worked_on'], [])
        self.assertEqual(worker_data['created_at'], '2000-01-01 00:00:00')
        self.assertEqual(worker_data['updated_at'], '2000-01-01 00:00:00')
        self.assertEqual(worker_data['curr_work_item_id'], None)
    
    def test_it_deletes_a_worker(self):
        with self.assertRaises(WorkerNotFoundError):
            self.service.delete_worker(1)
        worker_data = self.service.create_worker('test-worker')
        self.service.delete_worker(1)
    
    def test_it_fails_to_delete_a_busy_worker(self):
        worker_data = self.service.create_worker('test-worker')
        worker = self.collection.get(1)
        worker.status = WorkerStatus.BUSY
        self.collection.update(worker)
        with self.assertRaises(WorkerIsBusyError):
            self.service.delete_worker(1)
    
    def test_it_lists_workers(self):
        worker_data1 = self.service.create_worker('test-worker-1')
        worker_data2 = self.service.create_worker('test-worker-2')
        worker_data3 = self.service.create_worker('test-worker-3')
        worker_data4 = self.service.create_worker('test-worker-4')
        workers_data = self.service.list_workers()
        self.assertEqual( [worker_data1, worker_data2, worker_data3, worker_data4], workers_data )

    def test_it_lists_workers_in_reverse(self):
        worker_data1 = self.service.create_worker('test-worker-1')
        worker_data2 = self.service.create_worker('test-worker-2')
        worker_data3 = self.service.create_worker('test-worker-3')
        worker_data4 = self.service.create_worker('test-worker-4')
        workers_data = self.service.list_workers(reverse=True)
        self.assertEqual( [worker_data4, worker_data3, worker_data2, worker_data1], workers_data )
        
        
class WorkItemServiceTest(unittest.TestCase):

    def setUp(self):    
        self.time_service = MockTimeService()
        self.sequence_mngr = MockSequenceManager()
        self.worker_sequence_name = 'SQ_WORKER'
        self.work_item_sequence_name = 'SQ_WORK_ITEM'
        self.sequence_mngr.ensure_sequences_exist([
            self.worker_sequence_name
        ,   self.work_item_sequence_name
        ])
        
        self.worker_collection = AutoIncrementCollection(
            MockCollection('WRKR', int_key=True)
        ,   self.sequence_mngr
        ,   self.worker_sequence_name
        )

        self.work_item_collection = AutoIncrementCollection(
            MockCollection('WKIT', int_key=True)
        ,   self.sequence_mngr
        ,   self.work_item_sequence_name
        )
        
        
        self.worker_service = WorkerService(
            self.worker_collection
        ,   self.time_service
        )
        
        self.service = WorkItemService(
            self.work_item_collection
        ,   self.worker_service
        ,   self.time_service
        )
        
    def tearDown(self):
        pass
            
    def test_it_creates_a_work_item(self):
        work_item_data = self.service.create_work_item(
            name = 'test-work-item', 
            payload = {'foo': 1, 'bar': 2}
        )
        #pprint(work_item_data)
        same_work_item_data = self.service.get_work_item(1)
        
        self.assertEqual(work_item_data, same_work_item_data)
        self.assertEqual(work_item_data['created_at' ],  '2000-01-01 00:00:00')
        self.assertEqual(work_item_data['error'      ],  None)
        self.assertEqual(work_item_data['id'         ],  1)
        self.assertEqual(work_item_data['name'       ],  'test-work-item')
        self.assertEqual(work_item_data['payload'    ],  {'bar': 2, 'foo': 1})
        self.assertEqual(work_item_data['retry_count'],  0)
        self.assertEqual(work_item_data['status'     ],  WorkItemStatus.READY)
        self.assertEqual(work_item_data['updated_at' ],  '2000-01-01 00:00:00')
        
    def test_it_deletes_a_work_item(self):
        with self.assertRaises(WorkItemNotFoundError):
            self.service.delete_work_item(1)
        work_item_data = self.service.create_work_item(
            name = 'test-work-item', 
            payload = {'foo': 1, 'bar': 2}
        )
        self.service.delete_work_item(1)
    
    def test_it_fails_to_delete_a_checked_out_work_item(self):
        _ = self.service.create_work_item(
            name = 'test-work-item', 
            payload = {'foo': 1, 'bar': 2}
        )
        work_item = self.work_item_collection.get(1)
        work_item.status = WorkItemStatus.CHECKED_OUT
        self.work_item_collection.update(work_item)
        with self.assertRaises(WorkItemIsCheckedOutError):
            self.service.delete_work_item(1)
    
    def test_it_assigns_work_to_a_worker(self):
        _ = self.worker_service.create_worker('test-worker')
        _ = self.service.create_work_item(
            name = 'test-work-item', 
            payload = {'foo': 1, 'bar': 2}
        )
        self.time_service.advance()
        
        self.service.assign_work(1)
        
        worker_data = self.worker_service.get_worker(1)
        work_item_data = self.service.get_work_item(1)
        self.assertEqual(worker_data['updated_at'], '2000-01-01 00:00:01')
        self.assertEqual(worker_data['status'], WorkerStatus.BUSY)
        self.assertEqual(worker_data['curr_work_item_id'], 1)
        self.assertEqual(work_item_data['updated_at'], '2000-01-01 00:00:01')
        self.assertEqual(work_item_data['status'], WorkItemStatus.CHECKED_OUT)
