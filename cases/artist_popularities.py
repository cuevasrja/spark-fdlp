from pyspark.sql import DataFrame
from utils.session import create_spark_session
from pyspark.sql.functions import avg, col, lit, concat
import matplotlib.pyplot as plt

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
            "popularity", col("popularity").cast("float")
        ).withColumn(
            "artist_popularity", col("artist_popularity").cast("float")
        ).filter(
            col("artist_name").isNotNull() & (col("artist_name") != "") &
            col("artist_popularity").isNotNull() &
            (col ("artist_popularity") > 0) &
            col("popularity").isNotNull() &
            (col("popularity") > 0)
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
        result.select("artist_name", "result").show(n=200, truncate=False)
        
        # Generar el gráfico de barras
        self.generate_bar_chart(result)
        
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
                AVG(CAST(artist_popularity AS FLOAT)) AS artist_popularity
            FROM tracks
            WHERE 
                artist_name IS NOT NULL
                AND artist_name != ''
                AND artist_popularity IS NOT NULL
                AND CAST(artist_popularity AS FLOAT) > 0
                AND popularity IS NOT NULL
                AND CAST(popularity AS FLOAT) > 0
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
        result.select("artist_name", "result").show(n=200, truncate=False)
        
        # Generar el gráfico de barras
        self.generate_bar_chart(result)
        
        return result
    
    def generate_bar_chart(self, result: DataFrame):
        """
        Generate a bar chart to visualize the relationship between artist popularity
        and their song average popularity.
        :param result: Spark DataFrame with the analysis results.
        """
        # Convertir el DataFrame de Spark a Pandas para usarlo con Matplotlib
        pandas_df = result.select("artist_name", "avg_song_popularity", "artist_popularity").toPandas()

        # Ordenamos por la populalridad del artista
        pandas_df = pandas_df.sort_values(by="artist_popularity", ascending=False).head(20)
        
        # Configurar el gráfico
        plt.figure(figsize=(15, 8))
        x = range(len(pandas_df["artist_name"]))
        width = 0.4  # Ancho de las barras

        # Barras para avg_song_popularity
        plt.bar(x, pandas_df["avg_song_popularity"], width=width, label="Avg Song Popularity", color="blue", alpha=0.7)

        # Barras para artist_popularity
        plt.bar([i + width for i in x], pandas_df["artist_popularity"], width=width, label="Artist Popularity", color="orange", alpha=0.7)
        
        # Calculamos y graficamos la diferencia entre artist_popularity y avg_song_popularity
        pandas_df["popularity_diff"] = pandas_df["artist_popularity"] - pandas_df["avg_song_popularity"]
        plt.plot([i + width/2 for i in x], pandas_df["popularity_diff"], label="Popularity Difference", color="red", marker="o", linewidth=2)
    
        # Configurar etiquetas y título
        plt.xticks([i + width / 2 for i in x], pandas_df["artist_name"], rotation=90)
        plt.xlabel("Artist")
        plt.ylabel("Popularity Score")
        plt.title("Comparación de Popularidad de Artista vs la Popularidad de sus Canciones")
        plt.legend()

        # Añadimos una línea para que sirva de referencia
        plt.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
        
        # Mostrar el gráfico
        plt.tight_layout()
        plt.show()
    
    def stop_session(self):
        """
        Stops the Spark session.
        """
        self.session.stop()
        print("\033[92;1mSpark session stopped\033[0m")