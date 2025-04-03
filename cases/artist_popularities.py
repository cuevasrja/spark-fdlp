from pyspark.sql import SparkSession, DataFrame
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
        self.session = SparkSession.builder \
            .appName("ArtistsPopularities") \
            .getOrCreate()
        
        # Load the dataset
        self.dataset = self.session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(filepath)
        
        # Clean and prepare the data
        self.dataset = self.dataset.withColumn(
            "artist_popularity", 
            when(col("artist_popularity").isNull() | (col("artist_popularity") == ""), 0)
            .otherwise(col("artist_popularity"))
        )
        
        # Convert to numeric types
        self.dataset = self.dataset.withColumn("popularity", col("popularity").cast("float"))
        self.dataset = self.dataset.withColumn("artist_popularity", col("artist_popularity").cast("float"))
    
    def dataframe_method(self):
        """
        Analyze the relationship between artists popularity and their songs popularity using DataFrames.
        """
        # Group by artist and calculate the average song popularity
        result = self.dataset.groupBy("artist_name") \
            .agg(
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
                AVG(popularity) AS avg_song_popularity,
                AVG(artist_popularity) AS artist_popularity
            FROM tracks
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
    
    def save_results(self, result: DataFrame, output_path: str):
        """
        Saves the results to a CSVfile.
        :param result: DataFrame with the results.
        :param output_path: Path where to save the results.
        """
        result.select("artist_name", "result").write \
            .option("header", "true") \
            .mode("overwrite") \
            .csv(output_path)
    
    def stop_session(self):
        """
        Stops the Spark session.
        """
        self.session.stop()
        print("\033[92;1mSpark session stopped\033[0m")