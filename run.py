""" Run the xetra application"""
import logging
import logging.config
import yaml
import os


def main():
    """
    Entry point to run xetra ETL job
    """
    # Parse yaml file
    path = os.getcwd() + '/configs/xetra_report1_config.yaml'
    config = yaml.safe_load(open(path))
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")


if __name__ == "__main__":
    main()
