""" Fetches a list of running AppServer instances on the local machine. """

import json

from appscale.common.monit_interface import MonitOperator, MonitStates


def instance_from_entry(entry):
  """ Extracts the version and port from an instance entry.

  Args:
    entry: A string containing a Monit instance entry.
  Returns:
    A tuple specifying the version key and port.
  """
  version, port = entry.rsplit('-', 1)
  version = version[len('app___'):]
  port = int(port)
  return version, port


def main():
  """ Fetches a list of running AppServer instances on the local machine. """
  monit_operator = MonitOperator()
  monit_entries = monit_operator.get_entries_sync()
  instances = [
    instance_from_entry(entry) for entry, state in monit_entries.items()
    if entry.startswith('app___')
    and state in (MonitStates.PENDING, MonitStates.RUNNING)]
  print(json.dumps(instances))
