"""
Helper class to update meta-file and extract information from meta-file
"""
import logging
import pandas as pd
from datetime import datetime, timedelta
from xetra.common.constants import MetaProcessFormat
from xetra.common.s3 import S3BucketConnector
from xetra.common.custom_exceptions import BadDateRange, WrongMetaFile


class MetaProcess():

    """
    Helper class to update meta-file and extract information from meta-file
    """
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def dt2str(self, arg_date):
        """
        Converts a datetime input to string with specific format

        @params arg_date: Argument date to be processed with format "%YYYY-%M-%D"
        """
        new_date = arg_date.strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        return new_date

    def calculate_datelist(self, arg_date):
        """
        Calulates a list of date from the argument date till today

        @params arg_date: Desired first date of the list with format "%YYYY-%M-%D"
        """
        min_date = (datetime.strptime(arg_date,
                    MetaProcessFormat.META_DATE_FORMAT.value).date() -
                    timedelta(days=MetaProcessFormat.META_DATE_DELTA.value))
        today_date = datetime.today().date()
        if today_date - min_date <= timedelta(days=0):
            self._logger.info("Argument date must be less than todays date")
            raise BadDateRange
        else:
            date_list = [(min_date + timedelta(days=x))
                         for x in range(0, (today_date - min_date).days + 1)]
        return date_list

    def read_meta_csv(self, s3_bucket_meta: S3BucketConnector, file: str, format: str):
        """
        Reads a csv file from the s3 bucket instance

        @params s3_bucket_meta: S3BucketConnector object, initialized
        @params file: file to be read
        """
        self._logger.info('Reading file %s/%s/%s', s3_bucket_meta.endpoint_url,
                          s3_bucket_meta._bucket.name, file)
        df_meta = s3_bucket_meta.read_s3_to_df(file, format)
        try:
            set_meta = set(pd.to_datetime(
                           df_meta[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value]).dt.date)
        except WrongMetaFile:
            self._logger.info('Metafile might be corrupted or does not exist')
        return df_meta, set_meta

    def update_meta_file(self, s3_bucket_meta: S3BucketConnector,
                         extract_date_list: list):
        """
        Update the metafile on the s3 bucket instance.

        @params s3_bucket_meta: S3BucketConnector object, initialized
        @params extract_date_list: list of dates to be updated in the meta file
        """
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value,
                              MetaProcessFormat.META_PROCESS_COLUMN.value])
        df_new[MetaProcessFormat.META_SOURCE_DATE_COLUMN.value] = extract_date_list
        df_new[MetaProcessFormat.META_PROCESS_COLUMN.value] = \
            datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            df_old, set_meta = self.read_meta_csv(s3_bucket_meta,
                                                  MetaProcessFormat.META_FILE_NAME.value,
                                                  MetaProcessFormat.META_FILE_FORMAT.value)
            self._logger.info(df_old)
            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            df_all = df_new
        s3_bucket_meta.write_df_to_s3(df_all,
                                      MetaProcessFormat.META_FILE_NAME.value,
                                      MetaProcessFormat.META_FILE_FORMAT.value)
        return True

    def return_date_list(self, s3_bucket_meta: S3BucketConnector, arg_date: str):
        """
        Returns a list of files to be processed. This happens in 4 steps:
            1. Comprehensive List: Generate a list of dates from the argument input date till today
            2. Metafile List: Read in metafile, generate a list of dates already processed.
            3. Pruned List: Prune the Comprehensive list by calculating intersection of 2 lists (sets)
            4. Find the earliest date in the Pruned list, return all dates since this date as a list.

        @params s3_bucket_meta: S3BucketConnector object, initialized
        @params arg_date: Desired first date of the list.
        """
        full_date_list = self.calculate_datelist(arg_date)
        try:
            df_meta, src_dates = self.read_meta_csv(s3_bucket_meta,
                                                    MetaProcessFormat.META_FILE_NAME.value,
                                                    MetaProcessFormat.META_FILE_FORMAT.value)
            dates_missing = set(full_date_list[1:]) - src_dates
            if dates_missing:
                min_date_pruned = min(set(full_date_list[1:]) - src_dates)
                date_list = self.calculate_datelist(self.dt2str(min_date_pruned))
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            date_list = full_date_list
            min_date_pruned = arg_date
        min_date_pruned = str(arg_date)
        date_list = [self.dt2str(date) for date in date_list]
        return min_date_pruned, date_list
