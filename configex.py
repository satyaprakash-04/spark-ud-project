from pyspark.sql import SparkSession
from library import get_spark_app_config
from library import Log4J


if __name__ == '__main__':
    conf = get_spark_app_config()
    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()
    logger = Log4J(spark)

    logger.warn('This is the warning message from spark configex.py')
    conf_string = spark.sparkContext.getConf()
    logger.info(conf_string.toDebugString())
    logger.info('Execution completed')
    spark.stop()