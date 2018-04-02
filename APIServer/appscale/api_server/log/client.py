from concurrent.futures import ThreadPoolExecutor

class LogClient(object):
    def __init__(self):
        self.thread_pool = ThreadPoolExecutor(4)
    def