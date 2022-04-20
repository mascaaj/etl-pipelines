""" Run the xetra application"""
import logging
import logging.config
import yaml
import os
from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformer import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    """
    Entry point to run xetra ETL job
    """
    # Parse yaml file
    path = os.getcwd() + '/configs/xetra_report1_config.yaml'
    config = yaml.safe_load(open(path))
    log_config = config['logging']
    s3_config = config['s3']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # Initialize the source and target args
    source = XetraSourceConfig(**config['source_config'])
    target = XetraTargetConfig(**config['target_config'])

    # # Instantiate the bucket connectors
    s3_bucket_src = S3BucketConnector(bucket=s3_config['s3_bucket_name_src'],
                                      secret_key=s3_config['s3_secret_key'],
                                      access_key=s3_config['s3_access_key'],
                                      endpoint_url=s3_config['s3_endpoint_url_src'])

    s3_bucket_trg = S3BucketConnector(bucket=s3_config['s3_bucket_name_trg'],
                                      secret_key=s3_config['s3_secret_key'],
                                      access_key=s3_config['s3_access_key'],
                                      endpoint_url=s3_config['s3_endpoint_url_trg'])

    # Run etl job
    xetra_etl = XetraETL(s3_bucket_src,
                         s3_bucket_trg,
                         s3_config['meta_key'],
                         source,
                         target)
    xetra_etl.etl_report1()
    logger.info("Xetra job has finished processing.")


if __name__ == "__main__":
    main()
