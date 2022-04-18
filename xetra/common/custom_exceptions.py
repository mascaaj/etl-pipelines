""" Custom Exceptions"""


class WrongFormatException(Exception):
    """
    WrongFormatException Class

    Exception that can be raised when the format
    type given is not yet supported
    """


class BadDateRange(Exception):
    """
    BadDateRange Class

    Exception that can be raised when
    date format input is incorrect
    argument date is after todays date
    and other date related issues
    """


class FileDoesNotExist(Exception):
    """
    FileDoesNotExist Class

    Exception that can be raised when
    the desired file is empty or does not exist
    """


class WrongMetaFile(Exception):
    """
    WrongMetaFile Class

    Exception for wrong meta file being read in
    """
