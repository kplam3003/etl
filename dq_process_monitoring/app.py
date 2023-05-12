# native libs
import sys
sys.path.append('../')

from datetime import datetime, timedelta
import time

# 3rd party
from croniter import croniter
import pandas as pd

# import scripts
import etl_lead_time_monitor
import etl_batch_review_monitor
import config

# logger
from core.logger import init_logger

logger = init_logger(config.LOGGER_NAME, config.LOGGER)


class Job:
    def __init__(self, jobs):
        self.jobs = jobs
        logger.info('[APP] Job object initiated')
    
    def handle_failed_job(self, ):
        pass
    
    def execute_jobs(self, fail_job_handler=None):
        logger.info('[APP] Executing jobs...')
        job_results = []
        for i, job in enumerate(self.jobs):
            try:
                _status = job()
            except Exception as e:
                logger.exception(f'[APP] Error happens during job number {i + 1} execution!')
                _status = -1
            finally:
                job_results.append(_status)
            
            # check result
            if _status != 1:
                if fail_job_handler is None:
                    fail_job_handler = self.handle_failed_job()
                else:
                    fail_job_handler()
                self.handle_failed_job()
            

def app():
    # define jobs
    logger.info('[APP] Initializing...')
    jobs = Job(SCRIPTS)
    
    # set up scheduler
    logger.info('[APP] Setting up scheduler...')
    logger.info(f'[APP] Cron schedule: {config.DQ_CRON_SCHEDULE}')
    base_time = datetime.utcnow()
    schedule = croniter(config.DQ_CRON_SCHEDULE, base_time)
    
    logger.info('[APP] Scheduler is now running...')
    if config.FIRST_EXECUTION:
        logger.info('[APP] First execution is set to True. Now executing...')
        jobs.execute_jobs()
        
    # get first execution time
    next_execution_time = pd.to_datetime(schedule.get_next(datetime))
    logger.info(f'[APP] Next execution will be at {next_execution_time}')
    
    # main scheduling loop
    while True:
        # check current time against execution time
        current_time = pd.to_datetime(datetime.utcnow()) # utc time
        if current_time >= next_execution_time:
            # execute jobs
            logger.info(f"[APP] It is {current_time}. Wake up!")
            jobs.execute_jobs()
            logger.info('[APP] Execution completed! Going back to sleep...')
            
            # get new execution plan
            next_execution_time = pd.to_datetime(schedule.get_next(datetime))
            logger.info(f'[APP] Next execution will be at {next_execution_time}')
            logger.info('[APP] Going back to sleep...')
        else:
            logger.info(f'[APP] Not yet time to execute jobs. Sleeping for {config.SLEEP} seconds...')
        # sleeeeepppp
        time.sleep(config.SLEEP)
            
        
if __name__ == '__main__':
    # a collection of script to be included in job
    SCRIPTS = [etl_lead_time_monitor.run, etl_batch_review_monitor.run]
    
    try:
        app()
    except KeyboardInterrupt:
        print('[APP] Interrupted!')
    
    
    
        
    
    
