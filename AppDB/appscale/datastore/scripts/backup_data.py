""" This process performs a backup of all the application entities for the
given app ID to the local filesystem.
"""
import argparse
import logging

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

from appscale.common import appscale_info, constants
from ..backup.datastore_backup import DatastoreBackup
from ..zkappscale import zktransaction as zk

# The location to look at in order to verify that an app is deployed.
_SOURCE_LOCATION = '/opt/appscale/apps/'

logger = logging.getLogger(__name__)


def init_parser():
  """ Initializes the command line argument parser.

  Returns:
    A parser object.
  """
  parser = argparse.ArgumentParser(
    description='Backup application code and data.')
  parser.add_argument('-a', '--app-id', required=True,
    help='the application ID to run the backup for')
  parser.add_argument('--source-code', action='store_true',
    default=False, help='backup the source code too. Disabled by default.')
  parser.add_argument('-d', '--debug', required=False, action="store_true",
    default=False, help='display debug messages')
  parser.add_argument('--skip', required=False, nargs="+",
    help='skip the following kinds, separated by spaces')

  return parser


def main():
  """ This main function allows you to run the backup manually. """

  parser = init_parser()
  args = parser.parse_args()

  # Set up logging.
  level = logging.INFO
  if args.debug:
    level = logging.DEBUG
  logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:' \
    '%(lineno)s %(message)s ', level=level)
  logger.info("Logging started")

  message = "Backing up "
  if args.source_code:
    message += "source and "
  message += "data for: {0}".format(args.app_id)
  logger.info(message)

  zk_connection_locations = appscale_info.get_zk_locations_string()
  retry_policy = KazooRetry(max_tries=5)
  zk_client = KazooClient(
    hosts=zk_connection_locations,
    connection_retry=constants.ZK_PERSISTENT_RECONNECTS,
    command_retry=retry_policy)
  zk_client.start()
  zookeeper = zk.ZKTransaction(zk_client)
  db_info = appscale_info.get_db_info()
  table = db_info[':table']

  skip_list = args.skip
  if not skip_list:
    skip_list = []
  logger.info("Will skip the following kinds: {0}".format(sorted(skip_list)))
  ds_backup = DatastoreBackup(args.app_id, zookeeper, table,
    source_code=args.source_code, skip_list=sorted(skip_list))
  try:
    ds_backup.run()
  finally:
    zookeeper.close()
