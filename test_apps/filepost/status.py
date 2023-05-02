import sys
import os
import os.path
import argparse
import shutil
import time
from honcho import Honcho
import honcho.util as util
import honcho.models as models
from textwrap import dedent

assert os.environ[ 'PYTHONUTF8' ] == "1"

logger = util.get_logger('status')

class StatusApp:
    
    VERSION = (0, 0, 0)
    WAIT_TIME = 10
    
    def __init__(self, db_path):
        self.pid = os.getpid()
        self.db_path = db_path
        self.last_counters = None
        
    def create_honcho(self):
        logger.info('creating honcho instance')
        self.honcho = Honcho(db_path=self.db_path)
        self.honcho.start()
    
    def get_data(self):
        logger.info(f"reading work item counters")
        try:
            self.honcho.begin_transaction(write=False)
            counters = self.honcho.work_item_service.get_counters()
            workers = self.honcho.worker_service.list_workers()
            self.honcho.commit_transaction()
            return counters, workers
        except:
            self.honcho.abort_transaction()
            raise
    
    def show_counters(self, counters):
        if self.last_counters is None:
            self.last_counters = counters
            
        counters['datetime']
        counters['ready']  
        counters['checked_out']
        counters['finished']
        counters['error']
        counters['total']
        
        datetime          = counters['datetime']
        ready             = counters['ready']  
        checked_out       = counters['checked_out']
        finished          = counters['finished']
        error             = counters['error']
        total             = counters['total']

        delta_ready       = counters['ready']           - self.last_counters['ready']  
        delta_checked_out = counters['checked_out']     - self.last_counters['checked_out']
        delta_finished    = counters['finished']        - self.last_counters['finished']
        delta_error       = counters['error']           - self.last_counters['error']
            
        msg = dedent(f"""
            ### WORK ITEM COUNTERS ###
             -> datetime    : {datetime}
             -> ready       : {ready} / {delta_ready}
             -> checked out : {checked_out} / {delta_checked_out}
             -> finished    : {finished} / {delta_finished}
             -> error       : {error} / {delta_error}
             -> total       : {total}
        """)
        logger.info(msg)
   
    def show_workers(self, workers):
        workers_txt = ["\n ### WORKERS ###"]
        now = self.honcho.time_service.now()
        for worker in workers:
            name = worker['name']
            delta_t = self.honcho.time_service.subtract(now, worker['updated_at'])
            status = worker['status']
            work_item_id = worker['curr_work_item_id']
            worker_txt = f" -> {name} :: status {status} / Work Item: {work_item_id} / delta_t: {delta_t}"
            workers_txt.append(worker_txt)
        txt = "\n".join(workers_txt)
        logger.info(txt)
        
    def run(self):
        logger.info('starting SCAA status - version %d.%d.%d', *self.VERSION)
        self.create_honcho()
        while True:
            counters, workers = self.get_data()
            self.show_counters(counters)
            self.show_workers(workers)
            self.last_counters = counters
            time.sleep(self.WAIT_TIME)
        logger.info('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_path', type=str, help='database path')
    args = parser.parse_args()
    app = StatusApp(args.db_path)
    app.run()