from appscale.api_server.log.messages import AppLog, LogQuery, RequestLog
from appscale.api_server.log import log_service_pb2
import unittest
import time
import base64

LOG_LEVEL_INFO = 1
LOG_LEVEL_WARNING = 2

MESSAGE_1 = 'show me what you got'

PROJECT = 'guestbook'

TIMESTAMP = time.time()
TIMESTAMP_USEC = int(TIMESTAMP * 1000 * 1000)


class TestAppLog(unittest.TestCase):
    def test_from_pb(self):
        # Test creating an AppLog from a LogLine.
        line_pb = log_service_pb2.LogLine()
        line_pb.time = TIMESTAMP_USEC
        line_pb.level = LOG_LEVEL_INFO
        line_pb.log_message = MESSAGE_1
        app_log = AppLog.from_pb(line_pb)
        self.assertEqual(app_log.time, TIMESTAMP_USEC)
        self.assertEqual(app_log.level, LOG_LEVEL_INFO)
        self.assertEqual(app_log.message, MESSAGE_1)

        # Test creating an AppLog from a UserAppLogLine.
        line_pb = log_service_pb2.UserAppLogLine()
        line_pb.timestamp_usec = TIMESTAMP_USEC
        line_pb.level = LOG_LEVEL_WARNING
        line_pb.message = MESSAGE_1
        app_log = AppLog.from_pb(line_pb)
        self.assertEqual(app_log.time, TIMESTAMP_USEC)
        self.assertEqual(app_log.level, LOG_LEVEL_WARNING)
        self.assertEqual(app_log.message, MESSAGE_1)

    def test_to_capnp(self):
        app_log = AppLog(TIMESTAMP_USEC, LOG_LEVEL_INFO, MESSAGE_1)
        line_capnp = app_log.to_capnp()
        self.assertEqual(line_capnp.time, TIMESTAMP_USEC)
        self.assertEqual(line_capnp.level, LOG_LEVEL_INFO)
        self.assertEqual(line_capnp.message, MESSAGE_1)
        # Ensure no exceptions are raised when serializing message.
        line_capnp.to_bytes()


class TestLogQuery(unittest.TestCase):
    def test_from_pb(self):
        request = log_service_pb2.LogReadRequest()
        request.app_id = PROJECT
        module_version = request.module_version.add()
        module_version.module_id = 'default'
        module_version.version_id = 'v1'
        request.start_time = TIMESTAMP_USEC
        request.end_time = TIMESTAMP_USEC
        request.offset.request_id = base64.b64encode('request1')
        request.minimum_log_level = LOG_LEVEL_INFO
        request.include_app_logs = True
        request.count = 5
        log_query = LogQuery.from_pb(request)
        self.assertEqual(log_query.service_id, 'default')
        self.assertEqual(log_query.version_id, 'v1')
        self.assertEqual(log_query.start_time, TIMESTAMP_USEC)
        self.assertEqual(log_query.end_time, TIMESTAMP_USEC)
        self.assertEqual(log_query.offset, 'request1')
        self.assertEqual(log_query.minimum_log_level, LOG_LEVEL_INFO)
        self.assertEqual(log_query.include_app_logs, True)
        self.assertListEqual(log_query.request_ids, [])
        self.assertEqual(log_query.count, 5)

    def test_to_capnp(self):
        log_query = LogQuery('default', 'v1')
        log_query.start_time = TIMESTAMP_USEC
        log_query.end_time = TIMESTAMP_USEC
        log_query.offset = 'request1'
        log_query.minimum_log_level = LOG_LEVEL_INFO
        log_query.include_app_logs = True
        log_query.count = 5
        capnp_query = log_query.to_capnp()
        self.assertEqual(capnp_query.startTime, TIMESTAMP_USEC)
        self.assertEqual(capnp_query.endTime, TIMESTAMP_USEC)
        self.assertEqual(capnp_query.offset, 'request1')
        self.assertEqual(capnp_query.minimumLogLevel, LOG_LEVEL_INFO)
        self.assertEqual(capnp_query.includeAppLogs, True)
        self.assertEqual(capnp_query.versionIds, [':'.join(['default', 'v1'])])
        self.assertEqual(capnp_query.requestIds, [])
        self.assertEqual(capnp_query.count, 5)
        self.assertEqual(capnp_query.reverse, False)
        # Ensure no exceptions are raised when serializing message.
        capnp_query.to_bytes()
