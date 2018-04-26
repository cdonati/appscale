import psycopg2

class PGPullQueue(object):
  def __init__(self):
    self.db_access = 