import os
import sys
import time

from tornado.testing import AsyncTestCase, gen_test

from .client import Datastore

APPSCALE_PYTHON_APPSERVER = os.path.realpath(
  os.path.join(os.path.abspath(__file__), '..', '..', '..', '..', 'AppServer'))
sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api.datastore import Entity, Key, Query, datastore_types

PROJECT_ID = 'guestbook'


class TestBasicOperations(AsyncTestCase):
  def setUp(self):
    super(TestBasicOperations, self).setUp()
    locations = os.environ['DATASTORE_LOCATIONS'].split()
    self.datastore = Datastore(locations, PROJECT_ID)

  def tearDown(self):
    self.tear_down_helper()
    super(TestBasicOperations, self).tearDown()

  @gen_test
  def tear_down_helper(self):
    pass
    # query = Query('Greeting', _app=PROJECT_ID)
    # results = yield self.datastore.run_query(query)
    # for entity in results:
    #   yield self.datastore.delete([entity.key()])

  @gen_test
  def test_put(self):
    entity = Entity('Greeting', name='test', _app=PROJECT_ID)
    entity['content'] = 'test'
    yield self.datastore.put([entity])
    txid = yield self.datastore.begin_transaction()
    yield self.datastore.delete([entity.key()])
    time.sleep(80)
    yield self.datastore.get(entity.key(), txid)
    # entity = Entity('Greeting', _app=PROJECT_ID)
    # entity = Entity('Greeting', name='long', _app=PROJECT_ID)
    # parent_key = Key.from_path('Guestbook', '1', _app=PROJECT_ID)
    # entity = Entity('Greeting', parent=parent_key, name='test',
    #                 _app=PROJECT_ID)
    # for index in range(6):
    #   entity = Entity('Greeting', name=str(index), _app=PROJECT_ID)
    #   entity['value'] = index
    # # entity['content'] = 'a' * 10000
    #   yield self.datastore.put(entity)

  # @gen_test
  # def test_query(self):
  #   query = Query(kind='Greeting', _app=PROJECT_ID)
  #   key = Key.from_path('Greeting', '1', _app=PROJECT_ID)
  #   end_key = Key.from_path('Greeting', '4', _app=PROJECT_ID)
  #   # key = Key.from_path('Greeting', '0'5692531021916715, _app=PROJECT_ID)
  #   # key = Key.from_path('Greeting2', 'tes', _app=PROJECT_ID)
  #   query['value >='] = 1
  #   query['value <'] = 4
  #   results = yield self.datastore.run_query(query)
  #
  #   print('keys:')
  #   for result in results:
  #     print(result.key().to_path())
  #
  #   print('results: {}'.format(results))

  # @gen_test
  # def test_get(self):
  #   # key = Key.from_path('Greeting', 6136743375239407, _app=PROJECT_ID)
  #   key = Key.from_path('Greeting', 'test', _app=PROJECT_ID)
  #   # key = Key.from_path('Greeting', 'long', _app=PROJECT_ID)
  #   entity = yield self.datastore.get(key)
  #   # self.assertEqual(entity['content'], 'a' * 10000)

  # @gen_test
  # def test_delete(self):
  #   key = Key.from_path('Greeting', 'test', _app=PROJECT_ID)
  #   yield self.datastore.delete([key])
