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
        self.work_item_finished_collection = MockCollection('WIDN', int_key=True)
        self.work_item_error_collection = MockCollection('WIER', int_key=True)
        
        self.worker_service = WorkerService(
            self.worker_collection
        ,   self.time_service
        )
        
        self.service = WorkItemService(
            self.work_item_collection
        ,   self.work_item_finished_collection
        ,   self.work_item_error_collection
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
        
        
    def test_it_finishes_work_assigned_to_a_worker(self):
        _ = self.worker_service.create_worker('test-worker')
        _ = self.service.create_work_item(
            name = 'test-work-item', 
            payload = {'foo': 1, 'bar': 2}
        )
        self.time_service.advance()
        self.service.assign_work(worker_id=1)
        self.time_service.advance()
        self.service.finish_work(worker_id=1)
        
        worker_data = self.worker_service.get_worker(1)
        with self.assertRaises(WorkItemNotFoundError):
            work_item_data = self.service.get_work_item(1)
        work_item_data = self.service.finished_collection.get(1).dict()
        
        self.assertEqual(worker_data['updated_at'], '2000-01-01 00:00:02')
        self.assertEqual(worker_data['status'], WorkerStatus.IDLE)
        self.assertEqual(worker_data['curr_work_item_id'], None)
        self.assertEqual(worker_data['worked_on'], [1])
        self.assertEqual(work_item_data['updated_at'], '2000-01-01 00:00:02')
        self.assertEqual(work_item_data['status'], WorkItemStatus.FINISHED)


    def test_it_marks_an_error_on_a_work_assigned_to_a_worker(self):
        _ = self.worker_service.create_worker('test-worker')
        _ = self.service.create_work_item(
            name = 'test-work-item', 
            payload = {'foo': 1, 'bar': 2}
        )
        self.time_service.advance()
        self.service.assign_work(worker_id=1)
        self.time_service.advance()
        self.service.mark_error(worker_id=1, error='there has been an error')
        
        worker_data = self.worker_service.get_worker(1)
        with self.assertRaises(WorkItemNotFoundError):
            work_item_data = self.service.get_work_item(1)
        work_item_data = self.service.error_collection.get(1).dict()
        
        self.assertEqual(worker_data['updated_at'], '2000-01-01 00:00:02')
        self.assertEqual(worker_data['status'], WorkerStatus.IDLE)
        self.assertEqual(worker_data['curr_work_item_id'], None)
        self.assertEqual(worker_data['worked_on'], [1])
        self.assertEqual(work_item_data['updated_at'], '2000-01-01 00:00:02')
        self.assertEqual(work_item_data['status'], WorkItemStatus.ERROR)
        self.assertEqual(work_item_data['error'], 'there has been an error')
    
    def test_it_retries_errored_work_items(self):
        worker_data = self.worker_service.create_worker('test-worker')
        wi1_data = self.service.create_work_item(name = 'test-work-item-1', payload = {'foo': 1, 'bar':  2} )
        wi2_data = self.service.create_work_item(name = 'test-work-item-2', payload = {'foo': 2, 'bar':  4} )
        wi3_data = self.service.create_work_item(name = 'test-work-item-3', payload = {'foo': 3, 'bar':  6} )
        wi4_data = self.service.create_work_item(name = 'test-work-item-4', payload = {'foo': 4, 'bar':  8} )
        wi5_data = self.service.create_work_item(name = 'test-work-item-5', payload = {'foo': 5, 'bar': 10} )
        wi6_data = self.service.create_work_item(name = 'test-work-item-6', payload = {'foo': 6, 'bar': 12} )
        
        self.time_service.advance()
        self.service.assign_work(worker_id=1)
        self.time_service.advance()
        self.service.mark_error(worker_id=1, error='there has been an error')
        
        self.time_service.advance()
        self.service.assign_work(worker_id=1)
        self.time_service.advance()
        self.service.mark_error(worker_id=1, error='there has been an error')

        self.time_service.advance()
        self.service.assign_work(worker_id=1)
        self.time_service.advance()
        self.service.mark_error(worker_id=1, error='there has been an error')
        
        work_items = self.service.collection.list()
        work_items_data = list([ wi.dict() for wi in work_items ])
        self.assertEqual(work_items_data, [wi4_data, wi5_data, wi6_data])
        
        self.service.retry_work_item(max_items=2)
        
        work_items = self.service.collection.list()
        work_items_data = list([ wi.dict() for wi in work_items ])
        self.assertEqual(len(work_items), 5)
        self.assertEqual(work_items[0].id, 1)
        self.assertEqual(work_items[0].retry_count, 1)
        self.assertEqual(work_items[0].status, WorkItemStatus.READY)
        self.assertEqual(work_items[1].id, 2)
        self.assertEqual(work_items[1].retry_count, 1)
        self.assertEqual(work_items[1].status, WorkItemStatus.READY)
        
    def test_it_times_out_workers(self):
        worker_data = self.worker_service.create_worker('test-worker')
        work_item_data = self.service.create_work_item(name = 'test-work-ite1', payload = {'foo': 1, 'bar':  2} )
        self.time_service.advance()
        self.service.assign_work(worker_id=1)
        self.time_service.advance()
        
        time_out_results = self.service.time_out_workers(time_out=30)
        self.assertEqual(time_out_results, [])
        
        self.time_service.sleep(30)
        time_out_results = self.service.time_out_workers(time_out=30)
        self.assertEqual(len(time_out_results), 1)
        self.assertEqual(time_out_results[0]["worker_id"], 1)
        self.assertEqual(time_out_results[0]["work_item_id"], 1)
        self.assertEqual(time_out_results[0]["time_out"], 31)
        
        worker_data = self.worker_service.get_worker(1)
        self.assertEqual(worker_data["status"], WorkerStatus.IDLE)
        self.assertEqual(worker_data["curr_work_item_id"], None)
        self.assertEqual(worker_data["worked_on"], [1])

        work_item_data = self.service.get_errored_work_item(1)
        self.assertEqual(work_item_data["id"], 1)
        self.assertEqual(work_item_data["status"], WorkItemStatus.ERROR)
        self.assertEqual(work_item_data["error"], "worker #1 has been timed out while working on on work item #1")
