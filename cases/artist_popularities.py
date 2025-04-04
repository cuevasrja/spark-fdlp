from pyspark.sql import DataFrame
from utils.session import create_spark_session
from pyspark.sql.functions import avg, col, when, lit, concat

class ArtistsPopularities:
    """
    Analyzes the relationship between artists popularity and their songs popularity.
    """
    def __init__(self, filepath: str):
        """
        Initializies the ArtistPopularities class.
        :param filepath: Path to the Spotify CSV file.
        """
        self.session = create_spark_session("ArtistsPopularities")
        self.session.conf.set("spark.sql.debug.maxToStringFields", "1000")
        
        # Load the dataset
        self.dataset: DataFrame = self.session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(filepath)
    
    def dataframe_method(self):
        """
        Analyze the relationship between artists popularity and their songs popularity using DataFrames.
        """
        
        # Clean and prepare the data
        df = self.dataset.withColumn(
            "artist_popularity", 
            when(col("artist_popularity").isNull() | (col("artist_popularity") == ""), 0)
            .otherwise(col("artist_popularity"))
        ).withColumn(
            "popularity", col("popularity").cast("float")
        ).withColumn(
            "artist_popularity", col("artist_popularity").cast("float")
        ).filter(
            col("artist_name").isNotNull() & (col("artist_name") != "")
        )
        
        # Group by artist and calculate the average song popularity
        result = df.groupBy("artist_name").agg(
            avg("popularity").alias("avg_song_popularity"),
            avg("artist_popularity").alias("artist_popularity")
        )
        
        # Format the result
        result = result.withColumn(
            "result", 
            concat(
                col("artist_name").cast("string"),
                lit(", Average song popularity: "), 
                col("avg_song_popularity").cast("string"),
                lit(", Artist popularity: "), 
                col("artist_popularity").cast("string")
            )
        )
        
        # Show the results
        result.select("artist_name", "result").show(truncate=False)
        
        return result
    
    def sql_method(self):
        """
        Analyze the relationship between artists popularity and their songs popularity using SQL.
        """
        # Create a temporary view of the dataset
        self.dataset.createOrReplaceTempView("tracks")
        
        # Execute the SQL query
        query = """
            SELECT 
                artist_name,
                AVG(CAST(popularity AS FLOAT)) AS avg_song_popularity,
                AVG(CASE 
                    WHEN artist_popularity IS NULL OR artist_popularity = '' THEN 0 
                    ELSE CAST(artist_popularity AS FLOAT) 
                END) AS artist_popularity
            FROM tracks
            WHERE artist_name IS NOT NULL AND artist_name != ''
            GROUP BY artist_name
        """
        
        result = self.session.sql(query)
        
        # Format the result
        result = result.withColumn(
            "result", 
            concat(
                col("artist_name").cast("string"),
                lit(", Average song popularity: "), 
                col("avg_song_popularity").cast("string"),
                lit(", Artist popularity: "), 
                col("artist_popularity").cast("string")
            )
        )
        
        # Show the results
        result.select("artist_name", "result").show(truncate=False)
        
        return result
    
    def stop_session(self):
        """
        Stops the Spark session.
        """
        self.session.stop()
        print("\033[92;1mSpark session stopped\033[0m")