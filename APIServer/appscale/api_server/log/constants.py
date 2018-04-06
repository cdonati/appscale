""" Constants related to the LogService. """

from appscale.api_server.constants import ApplicationError
from appscale.api_server.log.log_service_pb2 import LogServiceError

LOG_SERVER_PORT = 7422


class ClientActions(object):
    """ Message types that the log server understands. """
    QUERY = 'q'
    SET_PROJECT = 'a'
    WRITE_LOG = 'l'


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
