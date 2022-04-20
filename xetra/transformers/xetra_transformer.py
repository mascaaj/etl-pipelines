"""
Xetra Data Core ETL Application layer
"""
import logging
import pandas as pd
from typing import NamedTuple
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from datetime import datetime
from xetra.common.constants import S3FileTypes


class XetraSourceConfig(NamedTuple):
    """
    Source data configuration & arguments
    """
    src_first_extract_date: str
    src_columns: list
    src_col_isin: str
    src_col_date: str
    src_col_time: str
    src_col_start_price: str
    src_col_max_price: str
    src_col_min_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Target data configuration & arguments
    """
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

    @params s3_bucket_source: S3 Source bucket
    @params s3_bucket_target: S3 Target bucket,
    @params meta_key: str <- might need to get rid of this one,
    @params src_args: source arguments for the pipeline,
    @params target_args: target arguments for the pipeline
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
        self.meta = MetaProcess()
        self.extract_date, self.extract_date_list = self.meta.return_date_list(self.s3_bucket_target,
                                                                               self.src_args.src_first_extract_date)
        self.meta_update_list = [date for date in self.extract_date_list if date >= self.extract_date]

    def extract(self):
        """
        Iterate thru datelist and for each file call list files in prefix function

        @todo : covert hardcoded file format types to params
        """
        self._logger.info("Extracting data from s3 bucket ...")
        files = [key for date in self.extract_date_list
                 for key in self.s3_bucket_source.list_files_in_prefix(date)]
        if not files:
            df = pd.DataFrame()
            self._logger.info("Dataframe empty")
        else:
            df = self.s3_bucket_source.read_s3_to_df(files[0], S3FileTypes.CSV.value)
            df = pd.concat([self.s3_bucket_source.read_s3_to_df(obj, S3FileTypes.CSV.value)
                            for obj in files], ignore_index=True)
        self._logger.info("Data extraction finished")
        return df

    def transform_report1(self, df: pd.DataFrame):
        """
        Transform the dataframe via grouping, aggregation and other operations to
        generate the output dataframe

        @params df: dataframe to be transformed / converted (output of extract stage)
        """
        if df.empty:
            self._logger.info("Dataframe is empty, no transformation will be applied")
            return df
        self._logger.info("Applying transformation to the Xetra source data - report 1")
        df = df.loc[:, self.src_args.src_columns]
        df.dropna(inplace=True)
        df[self.target_args.trg_col_op_price] = df.sort_values(by=[self.src_args.src_col_time])\
                                                  .groupby([self.src_args.src_col_isin,
                                                            self.src_args.src_col_date]
                                                           )[self.src_args.src_col_start_price].transform('first')
        df[self.target_args.trg_col_clos_price] = df.sort_values(by=[self.src_args.src_col_time])\
                                                    .groupby([self.src_args.src_col_isin,
                                                              self.src_args.src_col_date]
                                                             )[self.src_args.src_col_start_price].transform('last')
        df = df.groupby([self.src_args.src_col_isin, self.src_args.src_col_date],
                        as_index=False).agg(opening_price_eur=(self.target_args.trg_col_op_price, 'min'),
                                            closing_price_eur=(self.target_args.trg_col_clos_price, 'min'),
                                            minimum_price_eur=(self.src_args.src_col_min_price, 'min'),
                                            maximum_price_eur=(self.src_args.src_col_max_price, 'max'),
                                            daily_traded_volume=(self.src_args.src_col_traded_vol, 'sum'))
        df['prev_closing_price'] = df.sort_values(by=[self.src_args.src_col_date])\
                                     .groupby([self.src_args.src_col_isin]
                                              )[self.target_args.trg_col_op_price].shift(1)
        df[self.target_args.trg_col_ch_prev_clos] = ((df[self.target_args.trg_col_op_price] -
                                                      df['prev_closing_price']) /
                                                     df['prev_closing_price']) * 100
        df.drop(columns='prev_closing_price', inplace=True)
        df = df.round(decimals=2)
        df = df[df.Date >= self.extract_date].reset_index(drop=True)
        df.round(decimals=2)
        self._logger.info("Transformation complete")
        return df

    def load(self, df: pd.DataFrame):
        """
        Loads the data to an s3 bucket, updates meta file

        @params df: dataframe to be uploaded to the s3 bucket (output of transform stage)

        @todo : covert hardcoded file format types to params
        """
        target_key = self.target_args.trg_key +\
            datetime.today().strftime(self.target_args.trg_key_date_format) +\
            "." + self.target_args.trg_format
        self.s3_bucket_target.write_df_to_s3(df, target_key, S3FileTypes.PARQUET.value)
        self._logger.info("Xetra data sucessfully written.")
        self.meta.update_meta_file(self.s3_bucket_target,
                                   self.meta_update_list)
        self._logger.info("Meta file has been updated")
        return True

    def etl_report1(self):
        """
        Main ETL Function, acts as wrapper to other smaller functions
        """
        df = self.extract()
        df = self.transform_report1(df)
        self.load(df)
        return True
