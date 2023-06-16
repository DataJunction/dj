"""
defines exceptions used for backend parsing
"""
from datajunction_server.errors import DJException


class DJParseException(DJException):
    """Exception type raised upon problem creating a DJ sql ast"""
