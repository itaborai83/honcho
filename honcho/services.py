class WorkerService:
    
    def __init__(self):
        pass
    
    def create_worker(self):
        pass
    
    def list_workers(self):
        pass
    
    def get_worker(self):
        pass
    
    def update_worker(self):
        pass
    
    def delete_worker(self):
        pass
    
    
class QueueService:
    
    def __init__(self):
        pass
    
    def add_work_item(self, payload):
        pass
    
    def checkout_work_item(self, worker_id):
        pass
    
    def finish_work_item(self, worker_id, work_item_id):
        pass
    
    def mark_error(self, worker_id, work_item_id):
        pass
    
    def retry_work_item(self, work_item_id):
        pass
    
    def get_statistics(self):
        pass