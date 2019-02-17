import os
import sys

from tornado.testing import AsyncTestCase, gen_test

from .client import Datastore

APPSCALE_PYTHON_APPSERVER = os.path.realpath(
  os.path.join(os.path.abspath(__file__), '..', '..', '..', '..', 'AppServer'))
sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api.datastore import Entity, Key, Query

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

  # @gen_test
  # def test_put(self):
  #   entity = Entity('Greeting', name='test', _app=PROJECT_ID)
  #   yield self.datastore.put(entity)

  # @gen_test
  # def test_get(self):
  #   key = Key.from_path('Greeting', 6136743375239407, _app=PROJECT_ID)
  #   # key = Key.from_path('Greeting', 'test', _app=PROJECT_ID)
  #   yield self.datastore.get(key)

  @gen_test
  def test_delete(self):
    key = Key.from_path('Greeting', 'test', _app=PROJECT_ID)
    yield self.datastore.delete([key])
