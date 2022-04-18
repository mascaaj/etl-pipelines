"""
Test s3 connections and associated functions
"""
import os
import unittest
import boto3
from io import BytesIO
import pandas as pd
from moto import mock_s3
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException


class TestS3BucketConnections(unittest.TestCase):

    def setUp(self):
        """
        Initialize the s3 connection needed for testing
        """
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'

        # create aws access keys as environment variables
        os.environ[self.s3_access_key] = 'KEY1'
        os.environ[self.s3_secret_key] = 'KEY2'

        # create s3 bucket on the mocked s3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'})
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(bucket=self.s3_bucket_name,
                                                secret_key=self.s3_secret_key,
                                                access_key=self.s3_access_key,
                                                endpoint_url=self.s3_endpoint_url)

    def tearDown(self):
        """
        Teardown the s3 connection needed for testing
        """
        self.mock_s3.stop()

    def fixture_setup(self, df: bool = False, format: str = 'csv'):
        """
        Creates the input for the tests. Intended to be used once per test in conjunction
        with the fixture_teardown method.

        :params df: should the member variable self.df be initialized
        :params format: format of the files to be uploaded to mock_s3 csv or parquet
        """
        prefix_exp = 'prefix/'

        csv_content_1 = """col1,col2,col3
                            valA,valB,4
                            valD,valE,8"""
        csv_content_2 = """col1,col2,col3,col4
                            valA,valB,4,val
                            valD,valE,8,VAL
                            valF,valH,7,Val
                            valG,valI,9,vaL"""
        df_content = pd.DataFrame([['valA', 'valB', 4, 'val'],
                                  ['valD', 'valE', 8, 'VAL'],
                                  ['valF', 'valH', 7, 'Val'],
                                  ['valG', 'valI', 9, 'vaL']],
                                  columns=['col1', 'col2', 'col3', 'col4'])
        if df:
            self.df = df_content

        if format == S3FileTypes.CSV.value:
            key1_exp = f'{prefix_exp}test1.csv'
            key2_exp = f'{prefix_exp}test2.csv'
            self.s3_bucket.put_object(Body=csv_content_1, Key=key1_exp)
            self.s3_bucket.put_object(Body=csv_content_2, Key=key2_exp)
        elif format == S3FileTypes.PARQUET.value:
            key1_exp = f'{prefix_exp}test1.parquet'
            key2_exp = f'{prefix_exp}test2.parquet'
            out_buffer = BytesIO()
            df_content.to_parquet(out_buffer, index=False)
            self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key1_exp)
            self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key2_exp)
        return prefix_exp, key1_exp, key2_exp

    def fixture_teardown(self, key1, key2):
        """
        Destroys the inputs for the test created by the fixture_setup method.

        @params key1: first file name
        @params key2: second file name
        """
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1
                    },
                    {
                        'Key': key2
                    }
                ]
            }
        )

    def test_list_files_in_prefix_ok(self):
        """
        Test files with correct prefixes, mocked on s3 bucket
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup()
        # Method Execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup / Tear down
        self.fixture_teardown(key1_exp, key2_exp)

    def test_list_files_in_prefix_wrong(self):
        """
        Test files with incorrect prefixes, mocked on s3 bucket
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup()
        wrong_prefix = f'not-{prefix_exp}'
        # Method Execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(wrong_prefix)
        # Tests after method execution
        self.assertTrue(not list_result)
        # Cleanup / Tear down
        self.fixture_teardown(key1_exp, key2_exp)

    def test_read_csv(self):
        """
        Test reading csv files from mocked s3 bucket
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup()
        # content2 2,2
        test_value = 7
        log_expression = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket.name}/{key1_exp}'

        # Method Execution
        with self.assertLogs() as logm:
            df1 = self.s3_bucket_conn.read_s3_to_df(key1_exp, 'csv')
            df2 = self.s3_bucket_conn.read_s3_to_df(key2_exp, 'csv')
            self.assertIn(log_expression, logm.output[0])
        # Tests after method execution
        size1 = df1.size
        size2 = df2.size
        self.assertEqual(6, size1)
        self.assertEqual(16, size2)
        self.assertTrue(isinstance(df1, pd.DataFrame))
        self.assertTrue(isinstance(df2, pd.DataFrame))
        self.assertEqual(test_value, df2.iloc[2, 2])
        # Cleanup / Tear down
        self.fixture_teardown(key1_exp, key2_exp)

    def test_read_parquet(self):
        """
        Test reading parquet files from mocked s3 bucket
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup(format='parquet')
        # content2 2,2
        test_value = 7
        log_expression = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket.name}/{key1_exp}'

        # Method Execution
        with self.assertLogs() as logm:
            df1 = self.s3_bucket_conn.read_s3_to_df(key1_exp, 'parquet')
            df2 = self.s3_bucket_conn.read_s3_to_df(key2_exp, 'parquet')
            self.assertIn(log_expression, logm.output[0])
        # Tests after method execution
        size1 = df1.size
        size2 = df2.size
        self.assertEqual(16, size1)
        self.assertEqual(16, size2)
        self.assertTrue(isinstance(df1, pd.DataFrame))
        self.assertTrue(isinstance(df2, pd.DataFrame))
        self.assertEqual(test_value, df2.iloc[2, 2])
        # Cleanup / Tear down
        self.fixture_teardown(key1_exp, key2_exp)

    def test_write_df_to_s3_empty(self):
        """
        Test Writing data to an s3 bucket, using an empty dataframe
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup()
        log_expression = "Dataframe is empty. No files will be written to s3"
        # Test Init
        df_empty = pd.DataFrame()
        with self.assertLogs() as logm:
            self.s3_bucket_conn.write_df_to_s3(df_empty, key1_exp, 'csv')
            self.s3_bucket_conn.write_df_to_s3(df_empty, key2_exp, 'csv')
            self.assertIn(log_expression, logm.output[0])
            self.assertIn(log_expression, logm.output[1])
        self.fixture_teardown(key1_exp, key2_exp)

    def test_write_df_to_s3_csv(self):
        """
        Test writing to a s3 bucket as csv format
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup(df=True)
        # Test Init
        self.s3_bucket_conn.write_df_to_s3(self.df, key1_exp, 'csv')
        self.s3_bucket_conn.write_df_to_s3(self.df, key2_exp, 'csv')
        # Read Files
        df1 = self.s3_bucket_conn.read_s3_to_df(key1_exp, 'csv')
        df2 = self.s3_bucket_conn.read_s3_to_df(key2_exp, 'csv')
        # Assert Results
        self.assertTrue(self.df.equals(df1))
        self.assertTrue(self.df.equals(df2))
        # clean Up
        self.fixture_teardown(key1_exp, key2_exp)

    def test_write_df_to_s3_parquet(self):
        """
        Test Writing data to an s3 bucket as parquet format
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup(df=True)
        # Test Init
        self.s3_bucket_conn.write_df_to_s3(self.df, key1_exp, 'parquet')
        self.s3_bucket_conn.write_df_to_s3(self.df, key2_exp, 'parquet')
        # Read Files
        df1 = self.s3_bucket_conn.read_s3_to_df(key1_exp, 'parquet')
        df2 = self.s3_bucket_conn.read_s3_to_df(key2_exp, 'parquet')
        # Assert Results
        self.assertTrue(self.df.equals(df1))
        self.assertTrue(self.df.equals(df2))
        # clean Up
        self.fixture_teardown(key1_exp, key2_exp)

    def test_write_df_to_s3_wrong_format(self):
        """
        Test Writing data to an s3 bucket as parquet format
        """
        # Expected Results
        prefix_exp, key1_exp, key2_exp = self.fixture_setup(df=True)
        log_expression = "File format does not exist. No files will be writen to s3"
        exception_expression = WrongFormatException
        wrong_format = 'narcuet'
        # Test Init
        with self.assertLogs() as logm:
            with self.assertRaises(exception_expression):
                self.s3_bucket_conn.write_df_to_s3(self.df, key1_exp, wrong_format)
            self.assertIn(log_expression, logm.output[0])
        self.fixture_teardown(key1_exp, key2_exp)


if __name__ == "__main__":
    unittest.main()
