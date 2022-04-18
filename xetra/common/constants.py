"""
File to store the constants for the application
"""

from enum import Enum


class S3FileTypes(Enum):
    """
    Supported filetypes for s3 bucket connector
    """
    CSV = 'csv'
    PARQUET = 'parquet'


class MetaProcessFormat(Enum):
    """
    Supported filetypes for s3 bucket connector
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H%M%S'
    META_SOURCE_DATE_COLUMN = 'source_data'
    META_PROCESS_COLUMN = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
    META_FILE_NAME = 'meta_file.csv'
    META_DATE_DELTA = 1


class DataParams(Enum):
    """
    Supported filetypes for s3 bucket connector
    """
    CSV_SEPARATOR = ','
    CSV_ENCODING = 'utf-8'
