import cPickle
import glob
import logging
import multiprocessing
import random
import time

from appscale.common import appscale_info
from appscale.common.retrying import retry
from appscale.datastore.backup.datastore_backup import DatastoreBackup
from appscale.datastore.datastore_distributed import DatastoreDistributed
from appscale.datastore.cassandra_env.cassandra_interface import (
  DatastoreProxy, LARGE_BATCH_THRESHOLD)
from appscale.datastore.cassandra_env.utils import mutations_for_entity
from appscale.datastore.utils import group_for_key, tornado_synchronous
from appscale.datastore.zkappscale import zktransaction as zk
from appscale.datastore.zkappscale.transaction_manager import (
  TransactionManager)

from google.appengine.datastore import datastore_pb
from google.appengine.datastore import entity_pb

worker_ds_access = None


class EntityLoader(object):
  """ Iterates through a backup file to return batches of entities. """
  def __init__(self, project_id, backup_file, batch_size):
    """ Creates a new EntityLoader.

    Args:
      project_id: A string specifying a project ID.
      backup_file: A file object pointing to the backup file.
      batch_size: An integer specifying the batch size threshold in bytes.
    """
    self.project_id = project_id
    self.backup_file = backup_file
    self.batch_size = batch_size

  def __iter__(self):
    return self

  def next(self):
    """ Fetches the next batch of entities. """
    entities = []
    batch_bytes = 0
    while True:
      try:
        entity = cPickle.load(self.backup_file)
      except EOFError:
        if entities:
          return self.project_id, entities

        raise StopIteration()

      entities.append(entity)
      batch_bytes += len(entity)
      if batch_bytes > self.batch_size:
        return self.project_id, entities


def worker_init():
  """ Initialize datastore connections for all the worker processes. """
  global worker_ds_access
  zk_connection_locations = appscale_info.get_zk_locations_string()
  zookeeper = zk.ZKTransaction(host=zk_connection_locations)
  transaction_manager = TransactionManager(zookeeper.handle)
  worker_ds_access = DatastoreDistributed(
    DatastoreProxy(), transaction_manager, zookeeper=zookeeper)


@retry(max_retries=None, retrying_timeout=None)
def store_entity_batch(args):
  """ Stores the given entity batch.

  Args:
    args: A tuple containing the project ID and a list of entities to store.
  """
  project_id, entity_batch = args
  logging.info('Storing {} entities'.format(len(entity_batch)))

  # Convert encoded entities to EntityProto objects, change the app ID if
  # it's different than the original and encode again.
  new_entities_encoded = []
  ent_protos = []
  for entity in entity_batch:
    ent_proto = entity_pb.EntityProto()
    ent_proto.ParseFromString(entity)
    ent_proto.key().set_app(project_id)

    ent_protos.append(ent_proto)
    new_entities_encoded.append(ent_proto.Encode())
  logging.debug("Entities encoded: {0}".format(new_entities_encoded))

  # Create a PutRequest with the entities to be stored.
  put_request = datastore_pb.PutRequest()
  put_response = datastore_pb.PutResponse()
  for entity in new_entities_encoded:
    new_entity = put_request.add_entity()
    new_entity.MergeFromString(entity)
  logging.debug("Put request: {0}".format(put_request))

  put_sync = tornado_synchronous(worker_ds_access.dynamic_put)
  put_sync(project_id, put_request, put_response)
  return len(ent_protos)


@retry(max_retries=None, retrying_timeout=None)
def store_entity_batch_unsafe(args):
  """ Stores a list of entities without any safeguards.

  Unlike the normal write path, this does not create a transaction, lock the
  groups being stored, or check for existing data in order to update index
  entries. It should not be used unless the project being restored has no
  existing data and there are no other processes making datastore mutations.

  Args:
    args: A tuple containing the project ID and a list of entities to store.
  """
  project_id, entity_batch = args
  session = worker_ds_access.datastore_batch.session
  logging.info('Storing {} entities'.format(len(entity_batch)))

  for encoded_entity in entity_batch:
    entity = entity_pb.EntityProto(encoded_entity)
    encoded_group_key = group_for_key(entity.key()).Encode()
    txid = 1

    mutations = []
    mutations.extend(mutations_for_entity(entity, txid))

    mutations.append({'table': 'group_updates',
                      'key': bytearray(encoded_group_key),
                      'last_update': txid})

    statements_and_params = worker_ds_access.datastore_batch.\
      statements_for_mutations(mutations, txid)
    futures = [session.execute_async(statement, params)
               for statement, params in statements_and_params]
    for future in futures:
      future.result()

  return len(entity_batch)


class DatastoreRestore(object):
  """ Backs up all the entities for a set application ID. """

  # The amount of time to wait for SIGINT in seconds.
  SIGINT_TIMEOUT = 5

  # The number of entities retrieved in a datastore request.
  BATCH_SIZE = 100

  # Retry sleep on datastore error in seconds.
  DB_ERROR_PERIOD = 30

  # The amount of seconds between polling to get the restore lock.
  LOCK_POLL_PERIOD = 60

  def __init__(self, app_id, backup_dir, zoo_keeper, worker_count,
               use_safe_puts=True):
    """ Constructor.

    Args:
      app_id: A str, the application ID.
      backup_dir: A str, the location of the backup file.
      zoo_keeper: A ZooKeeper client.
      worker_count: An integer specifying how many worker processes to use.
      use_safe_puts: Use the normal datastore write path for put operations.
    """
    self.app_id = app_id
    self.backup_dir = backup_dir
    self.zoo_keeper = zoo_keeper

    self.entities_restored = 0
    self.indexes = []

    self._pool = multiprocessing.Pool(worker_count, initializer=worker_init)
    self._use_safe_puts = use_safe_puts

  def stop(self):
    """ Stops the restore process. """
    pass

  def run(self):
    """ Starts the main loop of the restore thread. """
    while True:
      logging.debug("Trying to get restore lock.")
      if self.get_restore_lock():
        logging.info("Got the restore lock.")
        self.run_restore()
        try:
          self.zoo_keeper.release_lock_with_path(zk.DS_RESTORE_LOCK_PATH)
        except zk.ZKTransactionException, zk_exception:
          logging.error("Unable to release zk lock {0}.".\
            format(str(zk_exception)))
        break
      else:
        logging.info("Did not get the restore lock. Another instance may be "
          "running.")
        time.sleep(random.randint(1, self.LOCK_POLL_PERIOD))

  def get_restore_lock(self):
    """ Tries to acquire the lock for a datastore restore.

    Returns:
      True on success, False otherwise.
    """
    return self.zoo_keeper.get_lock_with_path(zk.DS_RESTORE_LOCK_PATH)

  def read_from_file_and_restore(self, backup_file):
    """ Reads entities from backup file and stores them in the datastore.

    Args:
      backup_file: A str, the backup file location to restore from.
    """
    if self._use_safe_puts:
      store_entity_function = store_entity_batch
      # Keep the batch size small to account for index entries while trying to
      # stay under the large batch threshold.
      batch_size_threshold = LARGE_BATCH_THRESHOLD / 4
    else:
      store_entity_function = store_entity_batch_unsafe
      # The batch size is not as important for unsafe puts since they don't
      # use atomic batches.
      batch_size_threshold = LARGE_BATCH_THRESHOLD

    with open(backup_file, 'rb') as file_object:
      entity_loader = EntityLoader(self.app_id, file_object,
                                   batch_size_threshold)
      self.entities_restored += sum(
        self._pool.map(store_entity_function, entity_loader))

  def run_restore(self):
    """ Runs the restore process. Reads the backup file and stores entities
    in batches.
    """
    logging.info("Restore started")
    start = time.time()

    for backup_file in glob.glob('{0}/*{1}'.
        format(self.backup_dir, DatastoreBackup.BACKUP_FILE_SUFFIX)):
      if backup_file.endswith(".backup"):
        logging.info("Restoring \"{0}\" data from: {1}".\
          format(self.app_id, backup_file))
        self.read_from_file_and_restore(backup_file)

    time_taken = time.time() - start
    logging.info("Restored {0} entities".format(self.entities_restored))
    logging.info("Restore took {0} seconds".format(str(time_taken)))
