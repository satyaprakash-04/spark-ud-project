from pyspark.sql import SparkSession
from library import Log4J, get_spark_app_config
from programs import read_csv
from programs.read_csv import read_csv_dataset

if __name__ == '__main__':
    # Testing if Log is working or not
    spark = SparkSession \
        .builder \
        .appName('sparklogapp') \
        .master('local[3]')\
        .getOrCreate()
    logger = Log4J(spark)
    logger.info('Starting of spark app')
    logger.info('Ending of spark app')

    spark.stop()
    # Ends
    # Reading CSV file.
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
    spark.stop()

