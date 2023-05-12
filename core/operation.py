import signal
from core.logger import auto_logger


class GracefulKiller:
    kill_now = False
    clean_fn = None

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)


    #@auto_logger('exit_gracefully')
    def exit_gracefully(self, signum, logger=None):
        #logger.info(f'Received terminate signal {signum}')
        if self.clean_fn:
            self.clean_fn()
            #logger.info(f"Executed clean function: {self.clean_fn.__name__}")
        
        self.kill_now = True



def auto_killer(func):
    def wrapper(*args, **kwargs):
        killer = GracefulKiller()

        def set_before_kill_executor(executor_fn):
            killer.clean_fn = executor_fn
        
        return func(*args, **kwargs, set_terminator=set_before_kill_executor)
    
    return wrapper
