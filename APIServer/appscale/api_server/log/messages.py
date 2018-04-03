""" LogService message containers. """

from __future__ import division

import base64
import time
from collections import deque

import capnp

from appscale.api_server.log import log_service_pb2
from appscale.api_server.log.constants import InvalidRequest
from appscale.common.constants import LOGGING_CAPNP

capnp.remove_import_hook()
logging_capnp = capnp.load(LOGGING_CAPNP)


class LogQuery(object):
    """ Represents a query to the LogService. """
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
        """ Creates a new LogQuery object.

        Args:
            service_id: A string specifying the service ID.
            version_id: A string specifying the version ID.
        """
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
        """ Creates a new LogQuery from a LogReadRequest. """
        try:
            module_version = request.module_version[0]
        except IndexError:
            raise InvalidRequest('Request must contain module_version field')

        if not module_version.version_id:
            raise InvalidRequest('Request must set module_version.version_id')

        conflicting_fields = {'request_id': ['start_time', 'end_time', 'offset']}
        for field, conflicting_fields in conflicting_fields.items():
            for conflicting_field in conflicting_fields:
                if request.HasField(field) and request.HasField(conflicting_field):
                    raise InvalidRequest("{} and {} can't both be specified".
                                         format(field, conflicting_field))

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
        """ Creates a new capnp Query object. """
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


class AppLog(object):
    """ Represents an application log line. """
    __slots__ = ['time', 'level', 'message']

    def __init__(self, timestamp, level, message):
        """ Creates a new AppLog object.

        Args:
            timestamp: An integer specifying a unix timestamp in microseconds.
            level: An integer specifying a logging level.
            message: A string containing the log line.
        """
        self.time = timestamp
        self.level = level
        self.message = message

    @staticmethod
    def from_pb(app_log_pb):
        """ Creates a new AppLog from a LogLine. """
        return AppLog(app_log_pb.time, app_log_pb.level, app_log_pb.message)

    def to_capnp(self):
        """ Creates a new capnp AppLog object. """
        log_line = logging_capnp.AppLog.new_message()
        log_line.time = self.time
        log_line.level = self.level
        log_line.message = self.message
        return log_line

    def to_pb(self):
        """ Creates a new LogLine object. """
        log_line = log_service_pb2.LogLine()
        log_line.time = self.time
        log_line.level = self.level
        log_line.message = self.message
        return log_line


class RequestLog(object):
    """ Represents an application request log. """
    # The maximum number of application logs for a given request.
    MAX_APP_LOGS = 1000

    # Protobuffer fields that are named differently than the attributes.
    PROTO_FIELDS = {'app_id': 'project_id'}

    # Cap'n Proto fields that are named differently than the attributes.
    CAPNP_FIELDS = {'appId': 'project_id',
                    'versionId': 'version_id',
                    'requestId': 'request_id',
                    'startTime': 'start_time',
                    'endTime': 'end_time',
                    'httpVersion': 'http_version',
                    'responseSize': 'response_size',
                    'userAgent': 'user_agent',
                    'appLogs': 'app_logs'}

    def __init__(self, request_id, project_id, version_id, ip, nickname,
                 user_agent, host, method, resource, http_version):
        """ Creates a new RequestLog object.

        Args:
            request_id: A string specifying the request ID.
            project_id: A string specifying the project ID.
            version_id: A string specifying the version ID.
            ip: A string specifying the user's IP address.
            nickname: A string representing the user that made the request.
            user_agent: A string specifying the user agent.
            host: A string specifying the host that received the request.
            method: A string specifying the HTTP method.
            resource: A string specifying the path and parameters.
            http_version: A string specifying the HTTP version.
        """
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
        self.offset = None
        self.app_logs = deque(maxlen=self.MAX_APP_LOGS)

    @property
    def combined(self):
        """ A string representing the request as a combined log entry. """
        date = time.strftime('%d/%b/%Y:%H:%M:%S %z', time.localtime(self.end_time))
        return ('{ip} - {nickname} [{date}] "{method} {resource} {ver}" '
                '{status} {size} "{ua}"').format(
            ip=self.ip, nickname=self.nickname, date=date, method=self.method,
            resource=self.resource, ver=self.http_version, status=self.status,
            size=self.response_size, ua=self.user_agent)

    @staticmethod
    def from_capnp(log):
        """ Creates a RequestLog from a capnp RequestLog. """
        request_log = RequestLog(
            log.requestId, log.appId, log.versionId, log.ip, log.nickname,
            log.userAgent, log.host, log.method, log.resource, log.httpVersion)

        request_log.status = log.status
        request_log.response_size = log.responseSize
        request_log.start_time = log.startTime / 1000 / 1000
        request_log.end_time = log.endTime / 1000 / 1000
        if log.offset:
            request_log.offset = log.offset

        request_log.app_logs.extend(
            [AppLog(line.time, line.level, line.message) for line in log.appLogs])
        return request_log

    def to_capnp(self):
        """ Creates a capnp RequestLog. """
        request_log = logging_capnp.RequestLog.new_message()
        for capnp_field in ['appId', 'versionId', 'requestId', 'ip', 'nickname',
                            'startTime', 'method', 'resource', 'httpVersion',
                            'userAgent', 'host']:
            field = self.CAPNP_FIELDS.get(capnp_field, capnp_field)
            setattr(request_log, capnp_field, getattr(self, field))

        app_logs = request_log.init('appLogs', len(self.app_logs))
        for index, line in self.app_logs:
            app_logs[index] = line.to_capnp()

        return request_log

    def to_pb(self, include_app_logs):
        """ Creates a protocol buffer RequestLog. """
        request_log = log_service_pb2.RequestLog()
        for pb_field in ['request_id', 'app_id', 'version_id', 'ip', 'nickname',
                         'user_agent', 'host', 'method', 'resource',
                         'http_version', 'status', 'response_size', 'combined']:
            field = self.PROTO_FIELDS.get(pb_field, pb_field)
            setattr(request_log, pb_field, getattr(self, field))

        request_log.start_time = int(self.start_time * 1000 * 1000)
        request_log.end_time = int(self.end_time * 1000 * 1000)
        if self.offset:
            request_log.offset = log_service_pb2.LogOffset()
            request_log.offset.request_id = base64.b64encode(self.offset)

        # TODO: Implement the following fields.
        request_log.latency = 0
        request_log.mcycles = 0
        request_log.url_map_entry = ''

        if include_app_logs:
            request_log.line = [line.to_pb() for line in self.app_logs]

        return request_log