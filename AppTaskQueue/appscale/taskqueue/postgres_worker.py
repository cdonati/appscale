import psycopg2
from tornado.locks import Semaphore
from tornado.httpclient import AsyncHTTPClient
import logging
from tornado.ioloop import IOLoop
from tornado import gen
import time

sem = Semaphore(28)
client = AsyncHTTPClient()
HOST = '192.168.33.16'
PORT = 10001

conn = psycopg2.connect("dbname='appscale_push_queues'")
pg_cur = conn.cursor()

class NoTasks(Exception):
    pass

task_count = 0

@gen.coroutine
def consume():
    pg_cur.execute("""
      UPDATE guestbook_queues
      SET time_available = CURRENT_TIMESTAMP + interval '10 minutes',
          lease_count = lease_count + 1
      WHERE (queue_name, task_name) = (
          SELECT queue_name, task_name
          FROM guestbook_queues
          WHERE queue_name = 'queue-1'
          AND time_available <= CURRENT_TIMESTAMP
          FOR UPDATE SKIP LOCKED
          LIMIT 1
      )
      RETURNING *;
    """)
    results = pg_cur.fetchall()
    if not results:
        raise NoTasks('No tasks left to lease')

    for result in results:
        queue_name = result[0]
        task_name = result[1]
        logging.warning('name: {}'.format(task_name))

        # with (yield sem.acquire()):
        #     yield client.fetch('http://{}:{}/worker'.format(HOST, PORT),
        #                        method='POST', body='')

        global task_count
        task_count += 1

        pg_cur.execute("""
            DELETE FROM guestbook_queues
            WHERE queue_name = %s
            AND task_name = %s
        """, (queue_name, task_name))
        conn.commit()


@gen.coroutine
def consume_all():
    while True:
        try:
            yield consume()
        except NoTasks:
            break


if __name__ == '__main__':
    logging.basicConfig()
    io_loop = IOLoop.current()
    start_time = time.time()
    io_loop.run_sync(consume_all)
    duration = time.time() - start_time
    logging.warning('{} tasks took {:.2f} sec'.format(task_count, duration))
    logging.warning('avg: {:.2f} tasks/sec'.format(task_count / duration))
