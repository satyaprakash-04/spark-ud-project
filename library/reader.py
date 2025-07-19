from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
FILE_DIR = os.path.join(BASE_DIR, 'datasets')

def read_screen_time_dataset(spark_obj):
    return spark_obj.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(os.path.join(FILE_DIR, 'screentime/screen_time.csv'))


def group_by_gender_st_gt_1(sc_df):
    return sc_df.where(sc_df['Average Screen Time (hours)'] > 1)\
            .select('Age','Gender','Screen Time Type','Day Type','Average Screen Time (hours)')\
            .groupBy('Gender').count()