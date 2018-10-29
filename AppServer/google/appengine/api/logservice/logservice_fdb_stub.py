"""Stub implementation for Log Service that uses sqlite."""

import time

import fdb

from google.appengine.api import apiproxy_stub
from google.appengine.api import appinfo
from google.appengine.api.logservice import log_service_pb
from google.appengine.runtime import apiproxy_errors

fdb.api_version(520)



_REQUEST_LOG_CREATE = """
CREATE TABLE IF NOT EXISTS RequestLogs (
  id INTEGER NOT NULL PRIMARY KEY,
  user_request_id TEXT NOT NULL,
  app_id TEXT NOT NULL,
  version_id TEXT NOT NULL,
  ip TEXT NOT NULL,
  nickname TEXT NOT NULL,
  start_time INTEGER NOT NULL,
  end_time INTEGER DEFAULT 0 NOT NULL,
  method TEXT NOT NULL,
  resource TEXT NOT NULL,
  http_version TEXT NOT NULL,
  status INTEGER DEFAULT 0 NOT NULL,
  response_size INTEGER DEFAULT 0 NOT NULL,
  user_agent TEXT NOT NULL,
  url_map_entry TEXT DEFAULT '' NOT NULL,
  host TEXT NOT NULL,
  task_queue_name TEXT DEFAULT '' NOT NULL,
  task_name TEXT DEFAULT '' NOT NULL,
  latency INTEGER DEFAULT 0 NOT NULL,
  mcycles INTEGER DEFAULT 0 NOT NULL,
  finished INTEGER DEFAULT 0 NOT NULL
);
"""

_APP_LOG_CREATE = """
CREATE TABLE IF NOT EXISTS AppLogs (
  id INTEGER NOT NULL PRIMARY KEY,
  request_id INTEGER NOT NULL,
  timestamp INTEGER NOT NULL,
  level INTEGER NOT NULL,
  message TEXT NOT NULL,
  FOREIGN KEY(request_id) REFERENCES RequestLogs(id)
);
"""


class LogServiceFDB(apiproxy_stub.APIProxyStub):
  """Python stub for Log Service service."""

  THREADSAFE = True

  _ACCEPTS_REQUEST_ID = True


  _DEFAULT_READ_COUNT = 20


  _MIN_COMMIT_INTERVAL = 5

  # The max number of bytes for each chunk in a log group.
  _CHUNK_SIZE = 10000

  def __init__(self, db, request_data=None):
    """Initializer.

    Args:
      db: An open FoundationDB database.
      request_data: A apiproxy_stub.RequestData instance used to look up state
        associated with the request that generated an API call.
    """

    super(LogServiceFDB, self).__init__('logservice',
                                        request_data=request_data)

    self._db = db
    self._fdb_logs_dir = fdb.directory.create_or_open(
      self._db, ('appscale', 'logservice'))

  def start_request(self, request_id, user_request_id, ip, app_id, version_id,
                    nickname, user_agent, host, method, resource, http_version,
                    start_time=None, module=None):
    """Starts logging for a request.

    Each start_request call must be followed by a corresponding end_request call
    to cleanup resources allocated in start_request.

    Args:
      request_id: A unique string identifying the request associated with the
        API call.
      user_request_id: A user-visible unique string for retrieving the request
        log at a later time.
      ip: The user's IP address.
      app_id: A string representing the application ID that this request
        corresponds to.
      version_id: A string representing the version ID that this request
        corresponds to.
      nickname: A string representing the user that has made this request (that
        is, the user's nickname, e.g., 'foobar' for a user logged in as
        'foobar@gmail.com').
      user_agent: A string representing the agent used to make this request.
      host: A string representing the host that received this request.
      method: A string containing the HTTP method of this request.
      resource: A string containing the path and query string of this request.
      http_version: A string containing the HTTP version of this request.
      start_time: An int containing the start time in micro-seconds. If unset,
        the current time is used.
      module: The string name of the module handling this request.
    """

    # In the SDK, the request ID value used during logservice API requests
    # (user_request_id, accessible via env['REQUEST_LOG_ID']) is different than
    # the request ID value used during API server calls. The same value is used
    # for both cases in this implementation.
    del user_request_id

    if module is None:
      module = appinfo.DEFAULT_MODULE

    if version_id is None:
      version_id = 'NO-VERSION'

    major_version_id = version_id.split('.', 1)[0]
    if start_time is None:
      start_time = self._get_time_usec()

    kwargs = {'service_id': module, 'version_id': major_version_id, 'ip': ip,
              'nickname': nickname, 'user_agent': user_agent, 'host': host,
              'method': method, 'resource': resource,
              'http_version': http_version}
    self._start_request(self._db, app_id, request_id, start_time, **kwargs)

  def end_request(self, project_id, request_id, status, response_size,
                  end_time=None):
    """Ends logging for a request.

    Args:
      project_id: A string specifying the request's project ID.
      request_id: A unique string identifying the request associated with the
        API call.
      status: An int containing the HTTP status code for this request.
      response_size: An int containing the content length of the response.
      end_time: An int containing the end time in micro-seconds. If unset, the
        current time is used.
    """
    if end_time is None:
      end_time = self._get_time_usec()

    kwargs = {'status': status, 'response_size': response_size}
    self._end_request(self._db, project_id, request_id, end_time, **kwargs)

  def _Dynamic_Flush(self, project_id, request_id, request):
    """Writes application-level log messages for a request."""
    self._insert_app_logs(self._db, project_id, request_id, request.logs())

  def _Dynamic_Read(self, request, response, project_id, request_id):

    # The current request ID is not required to satisfy the query.
    del request_id

    if (request.server_version_size() < 1 and
        request.version_id_size() < 1 and
        request.request_id_size() < 1):
      raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST)

    if request.server_version_size() > 0 and request.version_id_size() > 0:
      raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST)

    if (request.request_id_size() and
        (request.has_start_time() or request.has_end_time() or
         request.has_offset())):
      raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST)

    if request.request_id_size():
      logs = self._get_logs_by_id(
        self._db, project_id, request.request_id_list(),
        request.include_app_logs())
      for request_log in logs:
        log = response.add_log()
        log.MergeFrom(request_log)

      return

    logs = self._query_logs(self._db, request)
    for log_row in logs[:count]:
      log = response.add_log()
      self._fill_request_log(log_row, log, request.include_app_logs())
    if len(logs) > count:
      response.mutable_offset().set_request_id(str(logs[-2]['id']))


  @staticmethod
  def _get_time_usec():
    return int(time.time() * 1e6)

  @fdb.transactional
  def _touch_last_update(self, tr, project_id, request_id, service_id=None,
                         version_id=None, update_time=None, remove_old=True):
    if update_time is None:
      update_time = self._get_time_usec()

    metadata = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'metadata'))

    if service_id is None:
      service_id = tr[metadata.pack((request_id, 'service_id'))]
      if not service_id.present():
        raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST, 'Request not found')

    if version_id is None:
      version_id = tr[metadata.pack((request_id, 'version_id'))]
      if not version_id.present():
        raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST, 'Request not found')

    last_update_index = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'last_update_index', service_id, version_id))

    if remove_old:
      previous_update = tr[metadata.pack((request_id, 'last_update'))]
      if not previous_update.present():
        raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST,
          'Request ID does not exist')

      previous_update = fdb.tuple.unpack(previous_update)[0]

      del tr[last_update_index.pack(previous_update, request_id)]

    tr[metadata.pack((request_id, 'last_update'))] = fdb.tuple.pack(
      (update_time,))
    tr[last_update_index.pack(update_time, request_id)] = b''

  @fdb.transactional
  def _start_request(self, tr, project_id, request_id, start_time, service_id,
                     version_id, **kwargs):
    metadata = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'metadata'))
    tr[metadata.pack((request_id, 'start_time'))] = fdb.tuple.pack(
      (start_time,))
    tr[metadata.pack((request_id, 'service_id'))] = service_id
    tr[metadata.pack((request_id, 'version_id'))] = version_id
    for key in ['ip', 'nickname', 'user_agent', 'host', 'method', 'resource',
                'http_version']:
      if key in kwargs:
        tr[metadata.pack((request_id, key))] = kwargs[key]

    self._touch_last_update(tr, project_id, request_id, service_id, version_id,
                            start_time, remove_old=False)

  @fdb.transactional
  def _insert_app_logs(self, tr, project_id, request_id, log_group):
    self._touch_last_update(tr, project_id, request_id)

    app_logs_dir = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'app_logs'))
    blob_length = len(log_group)
    if not blob_length:
      return

    chunk_indexes = [(n, n + self._CHUNK_SIZE)
                     for n in xrange(0, blob_length, self._CHUNK_SIZE)]
    for start, end in chunk_indexes:
      key = app_logs_dir.pack_with_versionstamp(
        (request_id, fdb.tuple.Versionstamp(), start))
      tr.set_versionstamped_key(key, log_group[start:end])

  @fdb.transactional
  def _end_request(self, tr, project_id, request_id, end_time, **kwargs):
    self._touch_last_update(tr, project_id, request_id, end_time)

    metadata = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'metadata'))

    tr[metadata.pack((request_id, 'end_time'))] = fdb.tuple.pack(
      (end_time,))
    for key in ['status', 'response_size']:
      if key in kwargs:
        tr[metadata.pack((request_id, key))] = fdb.tuple.pack((kwargs[key],))

  @fdb.transactional
  def _get_logs_by_id(self, tr, project_id, request_ids, include_app_logs):
    metadata_dir = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'metadata'))
    app_logs_dir = self._fdb_logs_dir.create_or_open(
      self._db, (project_id, 'app_logs'))

    request_logs = []
    for request_id in request_ids:
      metadata = tr.snapshot[metadata_dir.range((request_id,))]
      if not metadata:
        continue

      request_log = log_service_pb.RequestLog()
      request_log.set_app_id(project_id)
      request_log.set_request_id(request_id)
      int_fields = ['start_time', 'end_time', 'status', 'response_size']
      setters = {
        'start_time': request_log.set_start_time,
        'end_time': request_log.set_end_time,
        'service_id': request_log.set_module_id,
        'version_id': request_log.set_version_id,
        'ip': request_log.set_ip,
        'nickname': request_log.set_nickname,
        'user_agent': request_log.set_user_agent,
        'host': request_log.set_host,
        'method': request_log.set_method,
        'resource': request_log.set_resource,
        'http_version': request_log.set_http_version,
        'status': request_log.set_status,
        'response_size': request_log.set_response_size
      }
      for field, value in metadata:
        if field in int_fields:
          value = fdb.tuple.unpack(value)[0]

        setters[field](value)

      if include_app_logs:
        group_chunks = []
        group_id = None
        for chunk_key, value in tr.snapshot[app_logs_dir.range((request_id,))]:
          new_group_id = app_logs_dir.unpack(chunk_key)[1]
          if new_group_id != group_id:
            group = log_service_pb.UserAppLogGroup(''.join(group_chunks))
            for line in group.log_line_list():
              request_log.add_line().MergeFrom(line)

            group_chunks = []
            group_id = new_group_id

          group_chunks.append(value)

      request_logs.append(request_log)

    return request_logs

  @fdb.transactional
  def _query(self, tr, request):
    start_time = None
    if request.has_start_time():
      start_time = request.start_time()

    end_time = None
    if request.has_end_time():
      end_time = request.end_time()

    if request.has_count():
      limit = request.count()
    else:
      limit = self._DEFAULT_READ_COUNT

    metadata_dir = self._fdb_logs_dir.create_or_open(
      self._db, (request.app_id(), 'metadata'))

    index_directories = [
      self._fdb_logs_dir.create_or_open(
        self._db, (request.app_id(), 'last_update_index', mv.module_id(),
                   mv.version_id()))
      for mv in request.module_version_list()]

    if request.has_offset():
      request_id = request.offset().request_id()
      end_time = tr.snapshot[metadata_dir.pack((request_id, 'last_update'))]
      if not end_time.present():
        raise apiproxy_errors.ApplicationError(
          log_service_pb.LogServiceError.INVALID_REQUEST, 'Offset not found')

      end_time = fdb.tuple.unpack(end_time)[0]

    # Request logs should be returned in reverse chronological order by last
    # update time.
    def range_getter(directory):
      if start_time
    iterators = [
      iter(tr.snapshot.get_range(
        begin=dir.range().start if start_time is None else dir.pack((start_time,)),
        end=dir.pack((end_time,)),
                                 reverse=True))
      for dir in index_directories]

    candidates = [(iterator, next(iterator, None)) for iterator in iterators]
    candidates = [(iterator, fdb.tuple.unpack(candidate)[-2:])
                  for iterator, candidate in candidates
                  if candidate is not None]
    results = []
    while True:
      last_update = max(candidates, key=lambda c: c[1][0])
      newest = candidates.pop([c[1][0] for c in candidates].index(last_update))
      request_id = newest[1][1]
      if
      metadata = tr.snapshot[metadata_dir.range((request_id,))]

      if
      if not candidates:
        break

    for index_key, _ in tr.snapshot.get_range(0, end_time, reverse=True):
      request_id = last_update_index.unpack(index_key)[1]
      metadata = tr.snapshot[metadata_dir.range((request_id,))]
    start_time = 0
    if request.has_start_time():
      filters.append(('start_time >= ?', request.start_time()))
    if request.has_end_time():
      filters.append(('end_time < ?', request.end_time()))


    if request.has_offset():
      filters.append(('RequestLogs.id < ?', int(request.offset().request_id())))
    if not request.include_incomplete():
      filters.append(('finished = ?', 1))
    if request.has_minimum_log_level():
      filters.append(('AppLogs.level >= ?', request.minimum_log_level()))

    if request.server_version(0).has_server_id():
      server_version = ':'.join([request.server_version(0).server_id(),
                                 request.server_version(0).version_id()])
    else:
      server_version = request.server_version(0).version_id()
    filters = [('version_id = ?', server_version)]

    if request.has_minimum_log_level():
      query = ('SELECT * FROM RequestLogs INNER JOIN AppLogs ON '
               'RequestLogs.id = AppLogs.request_id%s GROUP BY '
               'RequestLogs.id ORDER BY id DESC')
    else:
      query = 'SELECT * FROM RequestLogs%s ORDER BY id DESC'

  def _fill_request_log(self, log_row, log, include_app_logs):
    log.set_request_id(str(log_row['user_request_id']))
    log.set_app_id(log_row['app_id'])
    log.set_version_id(log_row['version_id'])
    log.set_ip(log_row['ip'])
    log.set_nickname(log_row['nickname'])
    log.set_start_time(log_row['start_time'])
    log.set_host(log_row['host'])
    log.set_end_time(log_row['end_time'])
    log.set_method(log_row['method'])
    log.set_resource(log_row['resource'])
    log.set_status(log_row['status'])
    log.set_response_size(log_row['response_size'])
    log.set_http_version(log_row['http_version'])
    log.set_user_agent(log_row['user_agent'])
    log.set_url_map_entry(log_row['url_map_entry'])
    log.set_latency(log_row['latency'])
    log.set_mcycles(log_row['mcycles'])
    log.set_finished(log_row['finished'])
    log.mutable_offset().set_request_id(str(log_row['id']))
    time_seconds = (log_row['end_time'] or log_row['start_time']) / 10**6
    date_string = time.strftime('%d/%b/%Y:%H:%M:%S %z',
                                time.localtime(time_seconds))
    log.set_combined('%s - %s [%s] "%s %s %s" %d %d - "%s"' %
                     (log_row['ip'], log_row['nickname'], date_string,
                      log_row['method'], log_row['resource'],
                      log_row['http_version'], log_row['status'] or 0,
                      log_row['response_size'] or 0, log_row['user_agent']))
    if include_app_logs:
      log_messages = self._conn.execute(
          'SELECT timestamp, level, message FROM AppLogs '
          'WHERE request_id = ?',
          (log_row['id'],)).fetchall()
      for message in log_messages:
        line = log.add_line()
        line.set_time(message['timestamp'])
        line.set_level(message['level'])
        line.set_log_message(message['message'])

  def _Dynamic_SetStatus(self, unused_request, unused_response,
                         unused_request_id):
    raise NotImplementedError

  def _Dynamic_Usage(self, unused_request, unused_response, unused_request_id):
    raise apiproxy_errors.CapabilityDisabledError('Usage not allowed in tests.')
