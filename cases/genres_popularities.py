from pyspark.sql import DataFrame
from utils.session import create_spark_session
from pyspark.sql.functions import col, avg, when
import matplotlib.pyplot as plt

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
            (col("popularity") >= 0) &
            (col("album_popularity") >= 0)
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
        result.show(n=200, truncate=False)
        print(f"Total rows: {result.count()}")
        
        # Generar el gráfico de barras
        self.generate_bar_chart(result)

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
              AND popularity IS NOT NULL AND CAST(popularity AS INT) >= 0
              AND album_popularity IS NOT NULL AND CAST(album_popularity AS INT) >= 0
            GROUP BY genre_id
            ORDER BY genre_id
        """

        # Ejecutar la consulta principal y mostrar el resultado
        result = self.session.sql(query)
        result.show(n=200, truncate=False)
        print(f"Total rows: {result.count()}")

        # Generar el gráfico de barras
        self.generate_bar_chart(result)

    def generate_bar_chart(self, result: DataFrame):
        """
        Generate a bar chart to visualize the popularity analysis.
        :param result: Spark DataFrame with the analysis results.
        """
        # Convertir el DataFrame de Spark a Pandas para usarlo con Matplotlib
        pandas_df = result.toPandas()

        # Configurar el gráfico
        plt.figure(figsize=(15, 8))
        x = range(len(pandas_df["genre_id"]))
        width = 0.3  # Ancho de las barras

        # Barras para avg_song_popularity
        plt.bar(x, pandas_df["avg_song_popularity"], width=width, label="Avg Song Popularity", color="cyan", alpha=0.7)

        # Barras para avg_album_popularity
        plt.bar([i + width for i in x], pandas_df["avg_album_popularity"], width=width, label="Avg Album Popularity", color="magenta", alpha=0.7)

        # Línea para popularity_diff
        plt.plot([i + width / 2 for i in x], pandas_df["popularity_diff"], label="Popularity Diff", color="red", marker="o", linewidth=2)

        # Configurar etiquetas y título
        plt.xticks([i + width / 2 for i in x], pandas_df["genre_id"], rotation=90)
        plt.xlabel("Genre ID")
        plt.ylabel("Popularity")
        plt.title("Comparación de Popularidad de Canciones y Álbumes por Género")
        plt.legend()

        # Mostrar el gráfico
        plt.tight_layout()
        plt.show()

    def stop_session(self):
        """
        Stop the Spark session.
        """
        self.session.stop()
        print(f"\033[92;1mSpark session stopped\033[0m")