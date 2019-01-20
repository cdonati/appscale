""" A datastore implementation that uses FoundationDB. """
from __future__ import absolute_import

import logging

import fdb
fdb.api_version(600)

logger = logging.getLogger(__name__)


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB.
      This is experimental. Don't use it in production. """
  def __init__(self):
    self._db = None

  def start(self):
    self._db = fdb.open()

  def dynamic_put(self, project_id, put_request, put_response):
    for entity in put_request.entity_list():
      logger.info('entity: {}'.format(entity))
