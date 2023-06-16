"""
defines exceptions used for backend parsing
"""
from dj.errors import DJException


class DJParseException(DJException):
    """Exception type raised upon problem creating a DJ sql ast"""
