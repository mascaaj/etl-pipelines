"""
Xetra Data Core ETL Application layer
"""
import logging
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector


class XetraSourceConfig(NamedTuple):
    src_first_extract_date: str
    src_columns: list
    src_col_isin: str
    src_col_date: str
    src_col_time: str
    src_col_start_price: str
    src_col_max_price: str
    src_col_min_price: str
    src_col_end_price: str
    src_col_traded_volume: str


class XetraTargetConfig(NamedTuple):
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_daily_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL():

    """
    Class for ETL of Xetra Data
    """
    def __init__(self,
                 s3_bucket_source: S3BucketConnector,
                 s3_bucket_target: S3BucketConnector,
                 meta_key: str,
                 src_args: XetraSourceConfig,
                 target_args: XetraTargetConfig):
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_source = s3_bucket_source
        self.s3_bucket_target = s3_bucket_target
        self.meta_key = meta_key
        self.src_args = src_args
        self.target_args = target_args
        self.extract_date = []
        self.extract_date_list = []
        self.meta_update_list = []

    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass
