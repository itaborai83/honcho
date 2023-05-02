import sys
import os
import os.path
import argparse

from honcho import Honcho
import honcho.util as util

assert os.environ[ 'PYTHONUTF8' ] == "1"

logger = util.get_logger('enqueuefiles')

class FileEnqueuerApp:
    
    VERSION = (0, 0, 0)
    
    def __init__(self, db_path, input_directory, method, user, password, url):
        self.db_path         = db_path
        self.input_directory = input_directory
        self.method          = method
        self.user            = user
        self.password        = password
        self.url             = url
        self.honcho          = None
    
    def create_honcho(self):
        logger.info('creating honcho instance')
        self.honcho = Honcho(db_path=self.db_path)
        self.honcho.start()
    
    def validate_directory(self):
        logger.info(f"validating input directory")
        is_dir = os.path.isdir(self.input_directory)
        if not is_dir:
            msg = f"input directory {self.input_directory} is not a directory"
            logger.error(msg)
            sys.exit(1)

    def list_files(self):
        logger.info(f"listing input directory '{self.input_directory}' files")
        file_names = os.listdir(self.input_directory)
        file_paths = []
        for file_name in file_names:
            file_path = os.path.join(self.input_directory, file_name)
            if not os.path.isfile(file_path):
                logger.warning(f"file path '{file_path}' is a directory. skipping it ...")
                continue
            file_paths.append(file_path)
        return file_paths
    
    def create_work_items(self, file_paths):
        logger.info(f"creating work items for parallel work load distribution")
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            logger.info(f"enqueuing {file_name}")
            try:
                payload = { 
                    "file"      : file_path
                ,   "method"    : self.method
                ,   "url"       : self.url
                ,   "user"      : self.user
                ,   "password"  : self.password
                }
                
                self.honcho.begin_transaction(write=True)
                _ = self.honcho.work_item_service.create_work_item(
                    name    = file_name
                ,   payload = payload
                )
                self.honcho.commit_transaction()
            except:
                self.honcho.abort_transaction()
                raise
                
    def run(self):
        logger.info('starting Honcho file poster - version %d.%d.%d', *self.VERSION)
        self.validate_directory()
        self.create_honcho()
        file_paths = self.list_files()
        self.create_work_items(file_paths)
        logger.info('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_path', type=str, help='database path')
    parser.add_argument('input_directory', type=str, help='input directory')
    parser.add_argument('method', type=str, default="POST", choices=["POST", "PUT", "DELETE"], help='HTTP method')
    parser.add_argument('user', type=str, help="authentication user")
    parser.add_argument('password', type=str, help="authentication password")
    parser.add_argument('url', type=str, help="url")
    args = parser.parse_args()
    app = FileEnqueuerApp(args.db_path, args.input_directory, args.method, args.user, args.password, args.url)
    app.run()