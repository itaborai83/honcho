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
    
    def __init__(self, db_path, input_directory, output_directory):
        self.db_path = db_path
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.honcho = None
        self.current_work_item_id = 0
    
    def create_honcho(self):
        logger.info('creating honcho instance')
        self.honcho = Honcho(db_path=self.db_path)
        self.honcho.start()
    
    def validate_directories(self):
        logger.info(f"validating input directory")
        is_dir = os.path.isdir(self.input_directory)
        if not is_dir:
            msg = f"input directory {self.input_directory} is not a directory"
            logger.error(msg)
            sys.exit(1)

        logger.info(f"validating output directory")
        is_dir = os.path.isdir(self.output_directory)
        if not is_dir:
            msg = f"output directory {self.output_directory} is not a directory"
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
                self.honcho.begin_transaction(write=True)
                _ = self.honcho.work_item_service.create_work_item(
                    name    = file_name
                ,   payload = { "file": file_path, "output": self.output_directory }
                )

                self.honcho.commit_transaction()
            except:
                self.honcho.abort_transaction()
                raise
    def run(self):
        logger.info('starting SCAA file extractor - version %d.%d.%d', *self.VERSION)
        self.validate_directories()
        self.create_honcho()
        file_paths = self.list_files()
        self.create_work_items(file_paths)
            
        logger.info('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('db_path', type=str, help='database path')
    parser.add_argument('input_directory', type=str, help='input directory')
    parser.add_argument('output_directory', type=str, help='output directory')
    args = parser.parse_args()
    app = FileEnqueuerApp(args.db_path, args.input_directory, args.output_directory)
    app.run()