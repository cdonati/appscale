from appscale.api_server.log.messages import AppLog, LogQuery, RequestLog
from appscale.api_server.log import log_service_pb2
import unittest
import time

LOG_LEVEL_INFO = 1
LOG_LEVEL_WARNING = 2

MESSAGE_1 = 'show me what you got'

TIMESTAMP_USEC = int(time.time() * 1000 * 1000)


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
        self.assertEqual(line_capnp.level, LOG_LEVEL_WARNING)
        self.assertEqual(line_capnp.message, MESSAGE_1)
        # Ensure no exceptions are raised when serializing message.
        line_capnp.encoded_bytes()

    def test_to_log_line(self):
        # Test encoding to protobuffer.
        app_log = AppLog(TIMESTAMP_USEC, LOG_LEVEL_INFO, MESSAGE_1)
        line_pb = app_log.to_log_line()
        self.assertEqual(line_pb.time, TIMESTAMP_USEC)
        self.assertEqual(line_pb.level, LOG_LEVEL_WARNING)
        self.assertEqual(line_pb.log_message, MESSAGE_1)
        # Ensure no exceptions are raised when serializing message.
        line_pb.SerializeToString()
