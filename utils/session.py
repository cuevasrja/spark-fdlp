from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session with the given application name.
    """
    spark = SparkSession.builder \
    .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.host", "192.168.0.103")\
        .getOrCreate()
    return spark