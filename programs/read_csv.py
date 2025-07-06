# from . import add_to_path
from pyspark.sql import SparkSession
import sys
from library import Log4J, get_spark_app_config


def get_file(logger_obj):
    if len(sys.argv) != 2:
        logger_obj.error('Argument not provided')
        sys.exit(1)
    return sys.argv[1]


def read_csv_dataset(spark_obj, logger_obj):
    return spark_obj.read\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .csv(get_file(logger_obj))


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4J(spark)
    try:
        logger.info('spark session created.')
        df = read_csv_dataset(spark, logger)
        df.show()
        logger.info('dataframe displayed successfully')
    except Exception as ee:
        logger.error(repr(ee))