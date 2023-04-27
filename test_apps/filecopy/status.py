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

logger = util.get_logger('status')

class StatusApp:
    
    VERSION = (0, 0, 0)
    WAIT_TIME = 2
    
    def __init__(self, db_path):
        self.pid = os.getpid()
        self.db_path = db_path
        
    def create_honcho(self):
        logger.info('creating honcho instance')
        self.honcho = Honcho(db_path=self.db_path)
        self.honcho.start()
    
    def get_counters(self):
        logger.info(f"reading work item counters")
        try:
            self.honcho.begin_transaction(write=False)
            counters = self.honcho.work_item_service.get_counters()
            self.honcho.commit_transaction()
            return counters
        except:
            self.honcho.abort_transaction()
            raise
    
    def show_counters(self, counters):
        msg = f"""\n
            -> datetime    : {counters['datetime']}
            -> ready       : {counters['ready']}
            -> checked out : {counters['checked_out']}
            -> finished    : {counters['finished']}
            -> error       : {counters['error']}
            -> total       : {counters['total']}
        """
        logger.info(msg)
   
    def run(self):
        logger.info('starting SCAA status - version %d.%d.%d', *self.VERSION)
        self.create_honcho()
        while True:
            counters = self.get_counters()
            self.show_counters(counters)
            time.sleep(self.WAIT_TIME)
        logger.info('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_path', type=str, help='database path')
    args = parser.parse_args()
    app = StatusApp(args.db_path)
    app.run()