"""
defines exceptions used for backend parsing
"""

from http import HTTPStatus

from datajunction_server.errors import DJException


class DJParseException(DJException):
    """Exception type raised upon problem creating a DJ sql ast"""

    http_status_code: int = HTTPStatus.UNPROCESSABLE_ENTITY
