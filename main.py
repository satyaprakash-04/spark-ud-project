from pyspark.sql import SparkSession
from library import Log4J, get_spark_app_config
from library.reader import read_screen_time_dataset, group_by_gender_st_gt_1
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
        # df = read_csv_dataset(spark, logger)
        # df.printSchema()
        # df = df.filter(df.age > 50).select(df.age, df.sex, df.target) # Series of Transformation
        # df.show() # Action
        sc_df = read_screen_time_dataset(spark)
        sc_partitioned_df = sc_df.repartition(2)
        conv_df = group_by_gender_st_gt_1(sc_partitioned_df)
        # conv_df.show()
        logger.info(conv_df.collect())
        logger.info('dataframe displayed successfully')
        input('Press Enter to Exit')
    except Exception as ee:
        logger.error(repr(ee))
    spark.stop()
