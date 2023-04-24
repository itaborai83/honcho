import time
from datetime import datetime, timedelta

import lmdb

from honcho.models import *
from honcho.repository import *
from honcho.services import *
from honcho.exceptions import *
import honcho.util as util
import honcho.config as config

class Application:

    def __init__(self, db_path):
        self.logger                         = util.get_logger('application')
        self.db_path                        = db_path
        self.env                            = None
        self.txn                            = None
        self.time_service                   = None
        self.sequence_mngr                  = None
        self.counter_mngr                   = None
        self._worker_collection             = None
        self.worker_collection              = None
        self._work_item_collection          = None
        self.work_item_collection           = None
        self.work_item_finished_collection  = None
        self.work_item_error_collection     = None
        self.worker_service                 = None
        self.work_item_service              = None
    
    def start(self):
        # open lmdb environment
        self.logger.info('starting application')
        
        self.logger.info(f'opening LMDB environment {self.db_path}')
        self.env = lmdb.open(
            self.db_path
        ,   map_size        = config.MAP_SIZE
        ,   max_spare_txns  = config.MAX_SPARE_TXNS
        )
        
        self.logger.info('creating time service')
        self.time_service = TimeService()
        
        self.logger.info('creating counter manager')
        self.counter_mngr = LmdbCounterManager()
        
        self.logger.info('creating sequence manager')
        self.sequence_mngr = LmdbSequenceManager()
        
        self.logger.info('creating worker collection')
        self._worker_collection = LmdbCollection('WRKR', int_key=True)
        self.worker_collection = AutoIncrementCollection(
            self._worker_collection
        ,   self.sequence_mngr
        ,   config.WORKER_SEQUENCE_NAME
        )
        
        self.logger.info('creating work item collection')
        self._work_item_collection = LmdbCollection('WKIT', int_key=True)
        self.work_item_collection = AutoIncrementCollection(
            self._work_item_collection
        ,   self.sequence_mngr
        ,   config.WORK_ITEM_SEQUENCE_NAME
        )
        self.work_item_finished_collection = LmdbCollection('WIFN', int_key=True)
        self.work_item_error_collection = LmdbCollection('WIER', int_key=True)
        
        self.logger.info('creating worker service')
        self.worker_service = WorkerService(
            self.worker_collection
        ,   self.time_service
        )
        
        self.logger.info('creating work item service')
        self.work_item_service = WorkItemService(
            collection          = self.work_item_collection
        ,   finished_collection = self.work_item_finished_collection
        ,   error_collection    = self.work_item_error_collection
        ,   worker_service      = self.worker_service
        ,   counter_mngr        = self.counter_mngr
        ,   time_service        = self.time_service
        )
        
        try:
            self.begin_transaction(write=True, reader_check=True)
            
            self.logger.info('ensuring sequences exist')
            self.sequence_mngr.ensure_sequences_exist([
                config.WORKER_SEQUENCE_NAME
            ,   config.WORK_ITEM_SEQUENCE_NAME
            ])
            self.logger.info('creating work items counters')
            self.counter_mngr.ensure_counters_exist([
                config.READY_COUNTER
            ,   config.CHECKED_OUT_COUNTER
            ,   config.FINISHED_COUNTER
            ,   config.ERROR_COUNTER
            ,   config.TOTAL_COUNTER
            ])           
            self.commit_transaction()
        except:
            self.abort_transaction()
            raise
        
    def begin_transaction(self, write=False, reader_check=False):
        self.logger.info('opening transaction')
        assert self.txn is None
        self.txn = self.env.begin(write=True)
        self.worker_service.txn = self.txn
        self.work_item_service.txn = self.txn
        if reader_check:
            self.env.reader_check()

    def commit_transaction(self):
        self.logger.info('commiting transaction')
        assert self.txn is not None 
        self.txn.commit()
        self.txn = None
        self.worker_service.txn = None
        self.work_item_service.txn = None
    
    def abort_transaction(self):
        self.logger.info('aborting transaction')
        assert self.txn is not None
        self.txn.abort()
        self.txn = None
        self.worker_service.txn = None
        self.work_item_service.txn = None

if __name__ == '__main__':
    app = Application(config.DB_PATH)
    app.start()
    