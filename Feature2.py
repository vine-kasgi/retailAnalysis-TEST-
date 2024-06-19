# Feature2
from pyspark.sql import SparkSession

def spark_session():
    return SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config('spark.shuffle.useOldFetchProtocol', 'true'). \
        enableHiveSupport(). \
        master('yarn'). \
        getOrCreate()
        