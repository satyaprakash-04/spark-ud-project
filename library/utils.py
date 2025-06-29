import configparser
from pathlib import Path
import os

from pyspark import SparkConf
BASE_DIR = Path(__file__).resolve().parent.parent
SPARK_CONF_LOCATION = os.path.join(BASE_DIR, 'spark.conf')


def get_spark_app_config():
    spark_conf = SparkConf()
    config_parser = configparser.ConfigParser()
    config_parser.read(SPARK_CONF_LOCATION)

    for key, value in config_parser.items('SPARK_APP_CONFIG'):
        print('key: ', key)
        print('value: ', value)
        spark_conf.set(key, value)
    return spark_conf


if __name__ == '__main__':
    get_spark_app_config()
