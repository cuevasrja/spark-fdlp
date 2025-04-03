from pyspark.sql import DataFrame
from utils.session import create_spark_session
from pyspark.sql.functions import col, avg, when

class GenresPopularities:
    """
    Analyze the relationship between genre popularity and song popularity.
    """

    def __init__(self, filepath: str):
        """
        Initialize the GenresPopularities class.
        :param filepath: Path to the input CSV file.
        """
        self.session = create_spark_session("GenresPopularities")
        self.session.conf.set("spark.sql.debug.maxToStringFields", "1000") 
        self.dataset: DataFrame = self.session.read.option("header", "true").option("inferSchema", "true").csv(filepath)

    def dataframe_method(self):
        """
        Calculate the difference between average song popularity and album popularity using DataFrame API.
        """
        # Filtrar las filas con valores válidos y excluir registros con genre_id nulo, vacío o igual a "0"
        df = self.dataset.filter(
            (col("genre_id").isNotNull()) & 
            (col("genre_id") != "") & 
            (col("genre_id") != "0") &
            (col("popularity").isNotNull()) &
            (col("album_popularity").isNotNull()) &
            (col("popularity") > 0) &
            (col("album_popularity") > 0)
        ).withColumn(
            "popularity", col("popularity").cast("int")
        ).withColumn(
            "album_popularity", col("album_popularity").cast("int")
        )

        # Agrupa por género y calcula los promedios
        result = df.groupBy("genre_id").agg(
            avg("popularity").alias("avg_song_popularity"),
            avg("album_popularity").alias("avg_album_popularity")
        ).withColumn(
            "popularity_diff",
            col("avg_song_popularity") - col("avg_album_popularity")
        ).orderBy("genre_id")

        # Muestra el resultado final
        result.show(n=100, truncate=False)
        print(f"Total rows: {result.count()}")

    def sql_method(self):
        """
        Calculate the difference between average song popularity and album popularity using SQL.
        """
        # Crear una vista temporal
        self.dataset.createOrReplaceTempView("songs")

        # Consulta principal que excluye registros con genre_id nulo o vacío
        query = """
            SELECT
                genre_id,
                AVG(CAST(popularity AS INT)) AS avg_song_popularity,
                AVG(CAST(album_popularity AS INT)) AS avg_album_popularity,
                AVG(CAST(popularity AS INT)) - AVG(CAST(album_popularity AS INT)) AS popularity_diff
            FROM songs
            WHERE genre_id IS NOT NULL AND genre_id != ''
              AND popularity IS NOT NULL AND CAST(popularity AS INT) > 0
              AND album_popularity IS NOT NULL AND CAST(album_popularity AS INT) > 0
            GROUP BY genre_id
            ORDER BY genre_id
        """

        # Ejecutar la consulta principal y mostrar el resultado
        result = self.session.sql(query)
        result.show(n=100, truncate=False)
        print(f"Total rows: {result.count()}")

    def stop_session(self):
        """
        Stop the Spark session.
        """
        self.session.stop()
        print(f"\033[92;1mSpark session stopped\033[0m")