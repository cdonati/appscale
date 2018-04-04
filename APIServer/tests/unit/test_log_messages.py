from appscale.api_server.log.messages import AppLog, LogQuery, RequestLog
from appscale.api_server.log import log_service_pb2
import unittest
import time

LOG_LEVEL_INFO = 1
LOG_LEVEL_WARNING = 2


class TestAppLog(unittest.TestCase):
    def test_from_pb(self):
        current_time_us = int(time.time() * 1000 * 1000)
        message = 'this is fine'

        # Test creating an AppLog from a LogLine.
        line_pb = log_service_pb2.LogLine()
        line_pb.time = current_time_us
        line_pb.level = LOG_LEVEL_INFO
        line_pb.log_message = message
        app_log = AppLog.from_pb(line_pb)
        self.assertEqual(app_log.time, current_time_us)
        self.assertEqual(app_log.level, LOG_LEVEL_INFO)
        self.assertEqual(app_log.message, message)

        # Test creating an AppLog from a UserAppLogLine.
        line_pb = log_service_pb2.UserAppLogLine()
        line_pb.timestamp_usec = current_time_us
        line_pb.level = LOG_LEVEL_WARNING
        line_pb.log_message = message
        app_log = AppLog.from_pb(line_pb)
        self.assertEqual(app_log.time, current_time_us)
        self.assertEqual(app_log.level, LOG_LEVEL_WARNING)
        self.assertEqual(app_log.message, message)

        # Test encoding to capnp.
        app_log = AppLog('asdf', LOG_LEVEL_INFO, message)
        line_capnp = app_log.to_capnp()
        line_capnp.encoded_bytes()
