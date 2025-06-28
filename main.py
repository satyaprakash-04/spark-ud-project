from pyspark.sql import SparkSession
from lib import Log4J
if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('sparklogapp') \
        .master('local[3]')\
        .getOrCreate()
    logger = Log4J(spark)
    logger.info('Starting of spark app')
    logger.info('Ending of spark app')

    spark.stop()

