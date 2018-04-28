from psycopg2 import sql

DB_NAME = 'appscale_pull_queues'


class PostgresPullQueue(object):
  def __init__(self, project_id, queue_name, pg_conn):
    self.project_id = project_id
    self._pg_cur = pg_conn.cursor()
    self._table = '_'.join([self.project_id, queue_name])

  def add_task(self, task):
    insert = sql.SQL(
      'INSERT INTO {} values (%s, %s)'.format(sql.Identifier(self._table)))
    params = [10, 20]
    self._pg_cur.execute(insert, params)
