"""
Test meta file processing and associated functions
"""
import os
import unittest
import boto3
from io import StringIO
import pandas as pd
from moto import mock_s3
from datetime import datetime, timedelta
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import BadDateRange, WrongMetaFile


class TestMetaFileProcessing(unittest.TestCase):

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
        self.meta = MetaProcess()

    def tearDown(self):
        """
        Teardown the s3 connection needed for testing
        """
        self.mock_s3.stop()

    def fixture_setup(self, datelist: bool = True, corrupt_file: bool = False):
        """
        Creates the input for the tests. Intended to be used once per test in conjunction
        with the fixture_teardown method.

        :params df: should the member variable self.df be initialized
        :params format: format of the files to be uploaded to mock_s3 csv or parquet
        """
        self.date_str_1 = '2022-04-12'
        self.date_str_2 = '3000-12-30'
        self.date_dt_1 = datetime.strptime(self.date_str_1,
                                           MetaProcessFormat.META_DATE_FORMAT.value)
        self.date_dt_2 = datetime.strptime(self.date_str_2,
                                           MetaProcessFormat.META_DATE_FORMAT.value)
        self.date_delta = (datetime.today().date() - self.date_dt_1.date()).days + 2
        self.date_delta_2 = (datetime.today().date() - self.date_dt_1.date()).days

        if not datelist:

            if corrupt_file:
                self.df_content = pd.DataFrame([['2022-04-02', '2022-04-02 12:33:23'],
                                                ['2022-04-04', '2022-04-02 12:33:23'],
                                                ['2022-04-01', '2022-04-02 12:33:23'],
                                                ['2022-04-03', '2022-04-02 12:33:23']],
                                               columns=['bad_column_name', 'datetime_of_processing'])
            else:
                self.df_content = pd.DataFrame([['2022-04-12', '2022-04-02 12:33:23'],
                                                ['2022-04-11', '2022-04-02 12:33:23'],
                                                ['2022-04-13', '2022-04-02 12:33:23'],
                                                ['2022-04-03', '2022-04-02 12:33:23']],
                                               columns=['source_data', 'datetime_of_processing'])
                self.datelist_old = list(self.df_content['source_data'])
            key1_exp = 'meta_file.csv'
            out_buffer = StringIO()
            self.df_content.to_csv(out_buffer, index=False)
            self.s3_bucket.put_object(Body=out_buffer.getvalue(), Key=key1_exp)
            return key1_exp

    def fixture_teardown(self, key1):
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
                    }
                ]
            }
        )

    def test_date_to_string_correct(self):
        """
        Test datetime to string conversion
        """
        # Expected Results
        self.fixture_setup()
        # Method Execution
        test_result_1 = self.meta.dt2str(self.date_dt_1)
        test_result_2 = self.meta.dt2str(self.date_dt_2)
        # Tests after method execution
        self.assertEqual(self.date_str_1, test_result_1)
        self.assertEqual(self.date_str_2, test_result_2)

    def test_date_list_bad_date_range(self):
        """
        Tests that the function returns an error if incorrect range is specified
        """
        # Expected Results
        self.fixture_setup()
        log_expression = 'Argument date must be less than todays date'
        exception_expression = BadDateRange
        # Test Init
        with self.assertLogs() as logm:
            with self.assertRaises(exception_expression):
                self.meta.calculate_datelist(self.date_str_2)
            self.assertIn(log_expression, logm.output[0])

    def test_date_list_good_date_range(self):
        """
        Tests that the function returns a date list as expected
        """
        # Expected Results
        self.fixture_setup(True)
        test_result_1 = self.meta.calculate_datelist(self.date_str_1)
        self.assertEqual(self.date_delta, len(test_result_1))
        self.assertEqual(type(datetime.today().date()), type(test_result_1[0]))

    def test_read_meta_file(self):
        """
        Tests if the metafile can be read as expected
        """
        key = self.fixture_setup(False)
        df_result, set_result = self.meta.read_meta_csv(self.s3_bucket_conn,
                                                        MetaProcessFormat.META_FILE_NAME.value,
                                                        MetaProcessFormat.META_FILE_FORMAT.value)
        self.assertTrue(isinstance(df_result, pd.DataFrame))
        self.assertEqual(self.df_content.iloc[1, 1], df_result.iloc[1, 1])
        self.fixture_teardown(key)

    def test_read_meta_file_corrupted(self):
        """
        Tests if the read metafile raises exception when it reads corrupted metafile
        """
        key = self.fixture_setup(False, True)
        exception_expression = WrongMetaFile
        with self.assertRaises(exception_expression):
            df_result, set_result = self.meta.read_meta_csv(self.s3_bucket_conn,
                                                            MetaProcessFormat.META_FILE_NAME.value,
                                                            MetaProcessFormat.META_FILE_FORMAT.value)
        self.fixture_teardown(key)

    def test_update_meta_file_correct(self):
        """
        Tests if the meta file has been updated correctly with correct inputs
        """
        key = self.fixture_setup(False)
        date_list = ['2022-02-12', '2022-02-13']
        self.meta.update_meta_file(self.s3_bucket_conn, date_list)
        self.new_datelist = self.datelist_old + date_list
        df_result, set_result = self.meta.read_meta_csv(self.s3_bucket_conn,
                                                        MetaProcessFormat.META_FILE_NAME.value,
                                                        MetaProcessFormat.META_FILE_FORMAT.value)
        self.assertListEqual(list(df_result['source_data']), self.new_datelist)
        self.fixture_teardown(key)

    def test_update_meta_file_no_metafile(self):
        """
        Tests if the meta file has been updated correctly if there was no metafile to begin with
        """
        date_list = ['2022-02-12', '2022-02-13']
        # proc_date_time = [datetime.today().date()] * 2
        self.meta.update_meta_file(self.s3_bucket_conn, date_list)
        df_result, set_result = self.meta.read_meta_csv(self.s3_bucket_conn,
                                                        MetaProcessFormat.META_FILE_NAME.value,
                                                        MetaProcessFormat.META_FILE_FORMAT.value)
        self.assertTrue(isinstance(df_result, pd.DataFrame))
        self.assertEqual(df_result.iloc[0, 0], date_list[0])
        self.assertEqual(df_result.iloc[1, 0], date_list[1])

    def test_return_date_list_no_metafile(self):
        """
        Tests if the datelist returns correctly if there was no metafile to begin with
        """
        key = self.fixture_setup()
        date_list_expected = [(datetime.today().date() - timedelta(days=x))
                              .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                              for x in range(self.date_delta)]
        min_date_result, date_list_result = self.meta.return_date_list(self.s3_bucket_conn,
                                                                       self.date_str_1)
        self.assertEqual(self.date_str_1, min_date_result)
        self.assertEqual(set(date_list_result), set(date_list_expected))
        self.fixture_teardown(key)

    def test_return_date_list_bad_date(self):
        """
        Tests if the datelist returns correctly if post dated argument is supplied
        """
        key = self.fixture_setup()
        exception_expression = BadDateRange
        # Test Init
        with self.assertRaises(exception_expression):
            self.meta.return_date_list(self.s3_bucket_conn, self.date_str_2)
        self.fixture_teardown(key)

    def test_return_date_list_with_metafile(self):
        """
        Tests if the datelist returns correctly metafile is supplied
        """
        key = self.fixture_setup(False)
        min_date_exp = '2022-04-12'
        date_list_expected = [(datetime.today().date() - timedelta(days=x))
                              .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                              for x in range(self.date_delta_2)]
        min_date_result, date_list_result = self.meta.return_date_list(self.s3_bucket_conn,
                                                                       self.date_str_1)
        self.assertEqual(set(date_list_expected), set(date_list_result))
        self.assertEqual(min_date_exp, min_date_result)
        self.fixture_teardown(key)


if __name__ == "__main__":
    unittest.main()

    # test = TestMetaFileProcessing()
    # test.setUp()
    # test.fixture_setup(False)
    # test.test_update_meta_file()
