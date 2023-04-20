import unittest

from honcho.models import *

class TestWorkItem(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
        
    def test_it_is_reseted_when_checked_out(self):
        work_item = WorkItem(id=1, name="Work Item 1", status=WorkItemStatus.CHECKED_OUT)
        work_item.reset()
        self.assertEqual(work_item.status, WorkItemStatus.READY)
        self.assertEqual(work_item.retry_count, 1)
        self.assertEqual(work_item.error, None)

    def test_it_fails_to_be_reseted_when_transient(self):
        work_item = WorkItem(id=0, name="Work Item 1", status=WorkItemStatus.CHECKED_OUT)
        with self.assertRaises(TransientWorkItemError):
            work_item.reset()

    def test_it_fails_to_be_reseted_when_ready(self):
        work_item = WorkItem(id=1, name="Work Item 1", status=WorkItemStatus.READY)
        with self.assertRaises(WorkItemShouldNotBeReadyError):
            work_item.reset()

    def test_it_fails_to_be_reseted_when_finished(self):
        work_item = WorkItem(id=1, name="Work Item 1", status=WorkItemStatus.FINISHED)
        with self.assertRaises(WorkItemShouldNotBeFinishedError):
            work_item.reset()
    
    def test_it_roundtrips_as_json(self):
        work_item = WorkItem(id=1, name="Work Item 1", status=WorkItemStatus.FINISHED)
        same_work_item = WorkItem.from_json(work_item.json())
        self.assertEqual(work_item, same_work_item)
        
class TestWorker(unittest.TestCase):
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
        
    def test_it_works_on_a_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        self.assertEqual(worker.curr_work_item_id, 1)
        self.assertEqual(worker.status, WorkerStatus.BUSY)
        self.assertIn(1, worker.worked_on)
        self.assertEqual(work_item.status, WorkItemStatus.CHECKED_OUT)
        
    def test_it_fails_to_work_when_transient(self):
        worker = Worker(id=0, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        with self.assertRaises(TransientWorkerError):
            worker.work_on(work_item)

    def test_it_fails_to_work_on_a_transient_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=0, name="Work Item 1")
        with self.assertRaises(TransientWorkItemError):
            worker.work_on(work_item)
    
    def test_it_finishes_working_on_a_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        worker.finish_work_on(work_item)

        self.assertEqual(worker.curr_work_item_id, None)
        self.assertEqual(worker.status, WorkerStatus.IDLE)
        self.assertIn(1, worker.worked_on)
        self.assertEqual(work_item.status, WorkItemStatus.FINISHED)
        self.assertEqual(work_item.error, None)

    def test_it_fails_finishing_work_on_a_transient_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        work_item.id = 0
        with self.assertRaises(TransientWorkItemError):
            worker.finish_work_on(work_item)
    
    def test_it_fails_finishing_work_as_a_transient_worker(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        worker.id = 0
        with self.assertRaises(TransientWorkerError):
            worker.finish_work_on(work_item)
    
    def test_it_fails_finishing_work_on_another_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item1 = WorkItem(id=1, name="Work Item 1")
        work_item2 = WorkItem(id=2, name="Work Item 2")
        worker.work_on(work_item1)
        with self.assertRaises(WorkItemNotReadyError):
            worker.finish_work_on(work_item2)
        
    def test_it_errors_working_on_a_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        worker.error_working_on(work_item, "unexpected error")

        self.assertEqual(worker.curr_work_item_id, None)
        self.assertEqual(worker.status, WorkerStatus.IDLE)
        self.assertIn(1, worker.worked_on)
        self.assertEqual(work_item.status, WorkItemStatus.ERROR)
        self.assertEqual(work_item.error, "unexpected error")

    def test_it_fails_erroring_work_on_a_transient_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        work_item.id = 0
        with self.assertRaises(TransientWorkItemError):
            worker.error_working_on(work_item, "unexpected error")
    
    def test_it_fails_finishing_work_as_a_transient_worker(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item = WorkItem(id=1, name="Work Item 1")
        worker.work_on(work_item)
        worker.id = 0
        with self.assertRaises(TransientWorkerError):
            worker.error_working_on(work_item, "unexpected error")
    
    def test_it_fails_finishing_work_on_another_work_item(self):
        worker = Worker(id=1, name="Test Worker 1")
        work_item1 = WorkItem(id=1, name="Work Item 1")
        work_item2 = WorkItem(id=2, name="Work Item 2")
        worker.work_on(work_item1)
        with self.assertRaises(WorkItemNotReadyError):
            worker.error_working_on(work_item2, "unexpected error")
    
    def test_it_roundtrips_as_json(self):
        worker = Worker(id=1, name="Test Worker 1")
        same_worker = Worker.from_json(worker.json())
        self.assertEqual(worker, same_worker)
        
if __name__ == '__main__':
    unittest.main()