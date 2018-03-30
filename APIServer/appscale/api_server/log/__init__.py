""" Implements the App Identity API. """

import logging

from appscale.api_server.log.log_service_pb2 import (
    FlushRequest,
    LogReadRequest,
    LogReadResponse)
from appscale.api_server.base_service import BaseService
from appscale.api_server.constants import CallNotFound

logger = logging.getLogger('appscale-api-server')


class LogService(BaseService):
    """ Implements the App Identity API. """
    SERVICE_NAME = 'logservice'

    LOG_SERVER_PORT = 7422

    # The appropriate messages for each API call.
    METHODS = {}

    def __init__(self, project_id, log_server_ip):
        """ Creates a new AppIdentityService.

        Args:
            project_id: A string specifying the project ID.
            log_server_ip: A string specifying the log server location.
        """
        super(LogService, self).__init__(self.SERVICE_NAME)

        self.project_id = project_id
        self.log_server_ip = log_server_ip

    def start_request(self, request_id):
    def sign(self, blob):
        """ Signs a message with the project's key.

        Args:
            blob: A binary type containing an arbitrary payload.
        Returns:
            A binary type containing the signed payload.
        Raises:
            UnknownError if the payload cannot be signed.
        """
        if self._key is None:
            raise UnknownError('A private key is not configured')

        return self._key.sign(blob)

    def make_call(self, method, encoded_request):
        """ Makes the appropriate API call for a given request.

        Args:
            method: A string specifying the API method.
            encoded_request: A binary type containing the request details.
        Returns:
            A binary type containing the response details.
        """
        if method not in self.METHODS:
            raise CallNotFound(
                '{}.{} does not exist'.format(self.SERVICE_NAME, method))

        request = self.METHODS[method][0]()
        request.ParseFromString(encoded_request)

        response = self.METHODS[method][1]()

        if method == 'SignForApp':
            response.key_name = self._key.key_name
            response.signature_bytes = self.sign(request.bytes_to_sign)
        elif method == 'GetPublicCertificatesForApp':
            public_certs = self.get_public_certificates()
            for public_cert in public_certs:
                cert = response.public_certificate_list.add()
                cert.key_name = cert.key_name = public_cert.key_name
                cert.x509_certificate_pem = public_cert.pem
        elif method == 'GetServiceAccountName':
            response.service_account_name = self.get_service_account_name()
        elif method == 'GetAccessToken':
            service_account_id = None
            if request.HasField('service_account_id'):
                service_account_id = request.service_account_id

            token = self.get_access_token(list(request.scope),
                                          service_account_id)

            response.access_token = token.token
            response.expiration_time = token.expiration_time

        return response.SerializeToString()
