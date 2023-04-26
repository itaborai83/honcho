import sys
import os
import os.path
import argparse
import shutil
import time
from honcho import Honcho
import honcho.util as util
import honcho.models as models

assert os.environ[ 'PYTHONUTF8' ] == "1"

logger = util.get_logger('enqueuefiles')

class FileEnqueuerApp:
    
    VERSION = (0, 0, 0)
    
    def __init__(self, db_path, worker_name, force):
        self.pid = os.getpid()
        self.db_path = db_path
        self.force = force
        self.retry = True
        self.worker_name = worker_name
        self.honcho = None
        self.worker_id = None
        self.worker = None
        self.work_item = None
        self.work_item_name = None
        self.work_item_id = None
        
    def create_honcho(self):
        logger.info('creating honcho instance')
        self.honcho = Honcho(db_path=self.db_path)
        self.honcho.start()
    
    def boot_worker(self):
        logger.info(f"initializing worker '{self.worker_name}'")
        def filter(worker):
            return worker.name == self.worker_name
        try:
            self.honcho.begin_transaction(write=True)

            workers = self.honcho.worker_service.list_workers(filter=filter)
            assert len(workers) < 2
            if len(workers) == 0:
                logger.info(f"creating worker '{self.worker_name}'")
                self.worker = self.honcho.worker_service.create_worker(self.worker_name)
                self.worker_id = self.worker["id"]
            else:
                logger.warning(f"worker '{self.worker_name}' already exists")
                self.worker = workers[0]
                self.worker_id = self.worker["id"]
                #logger.info(repr(self.worker))
                if self.worker['status'] == models.WorkerStatus.IDLE:
                    logger.info(f"worker '{self.worker_name}' is IDLE. proceeding")
                    
                else:
                    if not self.force:
                        logger.error(f"worker '{self.worker_name}' is busy. Is it running in another process?")
                        sys.exit(1)
                    else:
                        logger.warning(f"worker '{self.worker_name}' is busy but --force flag supplied")
                        logger.warning(f"canceling work on work item #'{self.worker['curr_work_item_id']}' is busy but --force flag supplied")
                        msg = f"worker '{self.worker_name}' was busy while booting agent on process pid:{self.pid}"
                        logger.warning(msg)
                        self.honcho.work_item_service.mark_error(self.worker_id, msg)
                        self.worker = self.honcho.worker_service.get_worker(self.worker_id)
                        if self.retry:
                            logger.warning('reenqueuing failed work items')
                            self.honcho.work_item_service.retry_work_item()
            self.honcho.commit_transaction()
        except:
            self.honcho.abort_transaction()
            raise

    def assign_work(self):
        logger.info(f"assigning work to '{self.worker_name}'")
        try:
            self.honcho.begin_transaction(write=True)
            self.work_item = self.honcho.work_item_service.assign_work(self.worker_id)
            self.work_item_name = self.work_item["name"]
            self.work_item_id = self.work_item["id"]
            logger.info(f"work item '{self.work_item_name}' / id: #{self.work_item_id} assigned to worker '{self.worker_name}'")
            #logger.info(self.work_item)
            self.honcho.commit_transaction()
        except:
            self.honcho.abort_transaction()
            raise
    
    def process_work(self):
        logger.info(f"processing work item '{self.work_item_name}'")
        error = None
        try:
            logger.info(self.work_item)
            src = self.work_item['payload']['file']
            filename = os.path.basename(src)
            dst_dir = self.work_item['payload']['output']
            dst = os.path.join(dst_dir, filename)
            shutil.copyfile(src, dst)
        except Exception as e:
            msg = f"an error ocurred while processing work item #{self.work_item_id}"
            logger.exception(msg)
            error = str(e)
            
        try:
            self.honcho.begin_transaction(write=True)
            if error is None:
                self.honcho.work_item_service.finish_work(self.worker_id)
            else:
                self.honcho.work_item_service.mark_error(self.worker_id, error)
            self.honcho.commit_transaction()
        except Exception as e:
            self.honcho.abort_transaction()
            raise
    
    def run(self):
        logger.info('starting SCAA file copier - version %d.%d.%d', *self.VERSION)
        self.create_honcho()
        self.boot_worker()
        while True:
            self.assign_work()
            self.process_work()
            #time.sleep(10)
        logger.info('finished')
            

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_path', type=str, help='database path')
    parser.add_argument('worker_name', type=str, help='worker name')
    parser.add_argument('--force', action='store_true', help='force busy worker to start')
    args = parser.parse_args()
    app = FileEnqueuerApp(args.db_path, args.worker_name, args.force)
    app.run()