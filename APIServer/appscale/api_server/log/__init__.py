""" Implements the App Identity API. """

import base64
import logging
import socket
import struct
import time

from collections import deque
import capnp
from tornado import gen


from appscale.api_server.log.log_service_pb2 import (
    FlushRequest,
    LogReadRequest,
    LogReadResponse,
    LogServiceError)
from appscale.api_server.base_service import BaseService
from appscale.api_server.constants import CallNotFound, ApplicationError
from appscale.common.constants import LOGGING_CAPNP

capnp.remove_import_hook()
logging_capnp = capnp.load(LOGGING_CAPNP)

logger = logging.getLogger('appscale-api-server')

# The maximum number of application logs for a given request.
MAX_APP_LOGS = 1000


class ClientActions(object):
    QUERY = 'q'
    SET_PROJECT = 'a'


class InvalidRequest(ApplicationError):
    """ Indicates that the request is invalid. """
    def __init__(self, message):
        self.detail = message
        self.code = LogServiceError.INVALID_REQUEST


class StorageError(ApplicationError):
    """ Indicates that there was a problem accessing the log storage layer. """
    def __init__(self, message):
        self.detail = message
        self.code = LogServiceError.STORAGE_ERROR


class RequestLog(object):
    def __init__(self, request_id, project_id, version_id, ip, nickname,
                 user_agent, host, method, resource, http_version):
        self.request_id = request_id
        self.project_id = project_id
        self.version_id = version_id
        self.ip = ip
        self.nickname = nickname
        self.user_agent = user_agent
        self.host = host
        self.method = method
        self.resource = resource
        self.http_version = http_version

        self.status = None
        self.response_size = None

        self.start_time = time.time()
        self.end_time = None
        self.app_logs = deque(maxlen=1000)


class LogQuery(object):
    # The number of request logs to fetch by default.
    DEFAULT_READ_COUNT = 20

    # Protobuffer fields that are named differently than the attributes.
    PROTO_FIELDS = {'request_id': 'request_ids'}

    # Cap'n Proto fields that are named differently than the attributes.
    CAPNP_FIELDS = {'startTime': 'start_time',
                    'endTime': 'end_time',
                    'minimumLogLevel': 'minimum_log_level',
                    'includeAppLogs': 'include_app_logs',
                    'requestIds': 'request_ids'}

    def __init__(self, service_id, version_id):
        self.service_id = service_id
        self.version_id = version_id
        self.start_time = None
        self.end_time = None
        self.offset = None
        self.minimum_log_level = None
        self.include_app_logs = False
        self.request_ids = []
        self.count = self.DEFAULT_READ_COUNT

    @classmethod
    def from_pb(cls, request):
        try:
            module_version = request.module_version[0]
        except IndexError:
            raise InvalidRequest('Request must contain module_version field')

        if not module_version.version_id:
            raise InvalidRequest('Request must set module_version.version_id')

        query = LogQuery(module_version.module_id, module_version.version_id)
        for pb_field in ['start_time', 'end_time', 'minimum_log_level',
                         'include_app_logs', 'request_id', 'count']:
            field = cls.PROTO_FIELDS.get(pb_field, pb_field)
            if request.HasField(pb_field):
                setattr(query, field, getattr(request, pb_field))

        if request.offset:
            query.offset = base64.b64decode(request.offset)

        return query

    def to_capnp(self):
        query = logging_capnp.Query.new_message()
        query.versionIds = [':'.join([self.service_id, self.version_id])]

        for capnp_field in ['startTime', 'endTime', 'offset',
                            'minimumLogLevel', 'includeAppLogs']:
            field = self.CAPNP_FIELDS.get(capnp_field, capnp_field)
            value = getattr(self, field)
            if value is not None:
                setattr(query, capnp_field, value)

        if self.request_ids:
            query.requestIds = self.request_ids
        else:
            query.count = self.count

        # GAE presents logs in reverse chronological order. This is not an
        # option available to users in GAE, so we always set it to True.
        query.reverse = True

        return query


class LogWriter(object):
    def __init__(self):
        pass
    def write_async(self):
        pass


class LogService(BaseService):
    """ Implements the App Identity API. """
    SERVICE_NAME = 'logservice'

    LOG_SERVER_PORT = 7422

    # The appropriate messages for each API call.
    METHODS = {'Flush': (FlushRequest, None),
               'LogRead': (LogReadRequest, LogReadResponse)}

    def __init__(self, project_id, log_server_ip, log_writer, thread_pool):
        """ Creates a new AppIdentityService.

        Args:
            project_id: A string specifying the project ID.
            log_server_ip: A string specifying the log server location.
            log_writer: A LogWriter object.
        """
        super(LogService, self).__init__(self.SERVICE_NAME)

        self.project_id = project_id
        self.log_server_ip = log_server_ip
        self.requests = {}
        self.writer = log_writer
        self.thread_pool = thread_pool

    def start_request(self, request_log):
        """ Starts logging for a request.

        Each start_request call must be followed by a corresponding end_request
        call to cleanup resources allocated in start_request.

        Args:
            request_log = A RequestLog object.
        """
        if request_log.request_id in self.requests:
            raise InvalidRequest('Request is already in progress')

    def end_request(self, request_id, status, response_size):
        """ Ends logging for a request.

        Args:
            request_id: A string specifying the request ID.
            status: An integer specifying the HTTP status code.
            response_size: An integer specifying the response length.
        """
        try:
            request_log = self.requests[request_id]
        except KeyError:
            raise InvalidRequest('Request not found')

        request_log.status = status
        request_log.response_size = response_size
        request_log.end_time = time.time()
        self.writer.write_async(request_log)

    @gen.coroutine
    def read(self, query):
        connection = yield self._connect()
        try:
            # Define the project for messages on this connection.
            yield self._send(ClientActions.SET_PROJECT, self.project_id,
                             connection)

            yield self._send(ClientActions.QUERY, query.to_capnp().to_bytes(),
                             connection)
        finally:
            connection.close()

        result_count = 0
        for rlBytes in self._query_log_server(rl.appId, packet):
            requestLog = logging_capnp.RequestLog.from_bytes(rlBytes)
            log = response.add_log()
            _fill_request_log(requestLog, log, request.include_app_logs())
            result_count += 1
            if result_count == count:
                response.mutable_offset().set_request_id(requestLog.offset)
        except:
            logging.exception("Failed to retrieve logs")
            raise apiproxy_errors.ApplicationError(
                log_service_pb.LogServiceError.INVALID_REQUEST)

    @gen.coroutine
    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            yield self.thread_pool.submit(
                sock.connect, (self.log_server_ip, self.LOG_SERVER_PORT))
        except socket.error:
            raise StorageError('Unable to connect to log server')

        raise gen.Return(sock)

    @gen.coroutine
    def _send(self, action, message, sock):
        message_length = struct.pack('I', len(message))
        full_message = ''.join([action, message_length, self.project_id])
        try:
            yield self.thread_pool.submit(sock.sendall, full_message)
        except socket.error:
            raise StorageError('Unable to send message to log server')

    def _receive(self, sock):
        chunks = []
        bytes_recd = 0
        while bytes_recd < MSGLEN:
            chunk = self.sock.recv(min(MSGLEN - bytes_recd, 2048))
            if chunk == '':
                raise RuntimeError("socket connection broken")
            chunks.append(chunk)
            bytes_recd = bytes_recd + len(chunk)
        return ''.join(chunks)
        sock.recv(4096)

    def _query_log_server(self, packet):

        log_server.send(packet)
        fh = log_server.makefile('rb')
        try:
            buf = fh.read(_I_SIZE)
            count, = struct.unpack('I', buf)
            for _ in xrange(count):
                buf = fh.read(_I_SIZE)
                length, = struct.unpack('I', buf)
                yield fh.read(length)
        finally:
            fh.close()
        self._release_logserver_connection(key, log_server)
    except socket.error, e:
        _cleanup_logserver_connection(log_server)
        raise

    def make_call(self, method, encoded_request, request_id):
        """ Makes the appropriate API call for a given request.

        Args:
            method: A string specifying the API method.
            encoded_request: A binary type containing the request details.
            request_id: A string specifying the request ID.
        Returns:
            A binary type containing the response details.
        """
        if method not in self.METHODS:
            raise CallNotFound(
                '{}.{} does not exist'.format(self.SERVICE_NAME, method))

        request = self.METHODS[method][0]()
        request.ParseFromString(encoded_request)

        response = self.METHODS[method][1]()

        if method == 'Flush':
            try:
                self.requests[request_id].app_logs.extend(request.logs)
            except IndexError:
                raise InvalidRequest('Request not found')
            return
        elif method == 'LogRead':
            service_id = request.module_version[0].module_id
            version_id = request.module_version[0].version_id
            response = self.METHODS[method][1]()
            return response.SerializeToString()
