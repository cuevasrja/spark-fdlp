from pyspark.sql import SparkSession

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a Spark session with the given application name.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .remote("sc://192.168.0.105:15002")\
        .getOrCreate()
    
    print(f"\033[92;1mSpark session created with app name: {app_name}\033[0m")
    return spark