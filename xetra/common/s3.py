"""
Connector & Methods accessing S3
"""

import os
import logging
import boto3
import pandas as pd
from io import StringIO, BytesIO
from xetra.common.constants import DataParams, S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class S3BucketConnector():
    """
    Class to interact with S3 Buckets
    """

    def __init__(self, bucket: str, secret_key: str, access_key: str, endpoint_url: str):
        """
        Constructor for S3BucketConnectorClass

        :param bucket: S3 bucket name for source data
        :param secret_key: secret key for accessing aws s3
        :param access_key: access key for accessing aws s3
        :param endpoint_url: endpoint url to s3
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        listing of files with a prefix on the s3 bucket
        :param prefix: prefix on the s3 bucket that should be filterd
        returns:
        all files with prefix key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_s3_to_df(self, key: str, format: str):
        """
        Reading a csv from s3 bucket and parsing it to a pandas dataframe
        :params key: Filename that is to be read to dataframe
        returns:
        pandas dataframe of the csv
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        if format == S3FileTypes.CSV.value:
            csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(DataParams.CSV_ENCODING.value)
            data = StringIO(csv_obj)
            df = pd.read_csv(data, delimiter=DataParams.CSV_SEPARATOR.value)
        elif format == S3FileTypes.PARQUET.value:
            prq_obj = self._bucket.Object(key=key).get().get('Body').read()
            data = BytesIO(prq_obj)
            df = pd.read_parquet(data)
        else:
            raise WrongFormatException
        return df

    def write_df_to_s3(self, df: pd.DataFrame, key: str, format: str):
        """
        Uploading a data file to a s3 bucket.
        Currently supports .csv & .parquet
        Cases:
            1. dataframe is empty
            2. dataframe exists, output csv
            3. dataframe exists, output parquet
            4. incorrect file extension

        :params df: dataframe to be uploaded
        :params key: name of file to be uploaded
        :params format: format of file to be uploaded (csv or parquet)
        """
        if df.empty:
            self._logger.info("Dataframe is empty. No files will be written to s3")
        elif format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer, index=False)
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        elif format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        else:
            self._logger.info("File format does not exist. No files will be writen to s3")
            raise WrongFormatException
        return True

    # def return_objects(self, arg_date: str, date_format: str):
    #     """
    #     Returns a list of filename from a given date onwards, from an s3 bucket
    #     This function might be obsolete and need to be removed.
    #     """
    #     arg_date_dt = datetime.strptime(arg_date, date_format).date() - timedelta(days=1)
    #     objects = [obj.key for obj in self._bucket.objects.all() if
    #                datetime.strptime(obj.key.split('/')[0], date_format).date() >= arg_date_dt]
    #     return objects
