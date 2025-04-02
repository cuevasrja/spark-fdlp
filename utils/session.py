from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session with the given application name.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .remote("spark://cuevasrja-endeavour:7077")\
        .getOrCreate()
    return spark