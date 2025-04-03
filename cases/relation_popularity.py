from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from utils.session import create_spark_session
from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns

class RelationPopularity:
    def __init__(self, filepath: str):
        """
        Initialize the RelationPopularity class.

        :param filepath: Path to the CSV file containing the data.
        """
        self.spark: SparkSession = create_spark_session("RelationPopularity")
        self.dataset: DataFrame = self.spark.read.option("header", "true").option("inferSchema", "true").csv(filepath)

    def dataframe_method(self):
        """
        Perform the operation using DataFrame API.
        """
        df: DataFrame = self.dataset

        # Only keep the columns acousticness, danceability, energy and popularity
        df = df.select("acousticness", "danceability", "energy", "popularity")

        # Convert the columns to float and popularity to int
        df = df.withColumn("acousticness", df["acousticness"].cast("float"))
        df = df.withColumn("danceability", df["danceability"].cast("float"))
        df = df.withColumn("energy", df["energy"].cast("float"))
        df = df.withColumn("popularity", df["popularity"].cast("int"))
        # Remove rows with null values
        df = df.na.drop()

        # Group popularity in clusters of 20. E.g. 0-20, 20-40, 40-60, 60-80, 80-100
        df = df.withColumn("rank", (df["popularity"] / 20).cast("int"))
        # Create a new column with the popularity cluster
        df = df.withColumn("rank", (df["rank"] * 20).cast("int"))
        # For popularity 100, add it to the cluster 80-100
        df = df.withColumn("rank", F.when(df["popularity"] == 100, 80).otherwise(df["rank"]))

        # Change the rank to a string. Example: From 0 to 0-20
        df = df.withColumn("rank", F.concat(df["rank"], F.lit("-"), (df["rank"] + 20)))

        # For each cluster, calculate the percentage in which each of the columns (acousticness, danceability, energy) is the maximum attribute of the row
        # Create a new column with the max value of the row. Write the name of the column in the new column
        df = df.withColumn("max_value", F.when(df["acousticness"] == F.greatest(df["acousticness"], df["danceability"], df["energy"]), "acousticness")
                        .when(df["danceability"] == F.greatest(df["acousticness"], df["danceability"], df["energy"]), "danceability")
                        .when(df["energy"] == F.greatest(df["acousticness"], df["danceability"], df["energy"]), "energy"))

        # Group by rank and max_value and count the number of rows in each group
        df = df.groupBy("rank", "max_value").count()
        # Calculate the percentage of each group
        df = df.withColumn("percentage", (df["count"] / F.sum(df["count"]).over(Window.partitionBy("rank"))) * 100)
        
        # Show the result
        df.show()

        # Save the result to a CSV file
        df.write.mode("overwrite").option("header", "true").csv("out/relation_popularity-dataframe")
        print(f"\033[92;1mCSV file saved\033[0m")

        # Plot the result
        df_pandas: pd.DataFrame = df.toPandas()

        df_pandas["rank"] = df_pandas["rank"].astype(str)
        df_pandas["max_value"] = df_pandas["max_value"].astype(str)
        df_pandas["percentage"] = df_pandas["percentage"].astype(float)
        df_pandas["rank"] = df_pandas["rank"].str.replace("-", " to ")
        df_pandas["max_value"] = df_pandas["max_value"].str.replace("acousticness", "Acousticness")
        df_pandas["max_value"] = df_pandas["max_value"].str.replace("danceability", "Danceability")
        df_pandas["max_value"] = df_pandas["max_value"].str.replace("energy", "Energy")

        # Plot the result as x: rank, y: percentage, hue: max_value
        # 3 lines (one for each max_value)
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=df_pandas, x="rank", y="percentage", hue="max_value", palette="Set1")
        plt.title("Popularity of each attribute")
        plt.xlabel("Popularity")
        plt.ylabel("Percentage")
        plt.legend(title="Attribute")
        plt.savefig("out/relation_popularity-dataframe.png")
        print(f"\033[92;1mImage saved\033[0m")

    def sql_method(self):
        """
        Perform the same operation using SQL.
        """
        # Create a temporary view of the dataset
        self.dataset.createOrReplaceTempView("songs")

        # Primary SQL query to select the relevant columns
        query = """
            SELECT 
                popularity,
                CAST(acousticness AS FLOAT) AS acousticness,
                CAST(danceability AS FLOAT) AS danceability,
                CAST(energy AS FLOAT) AS energy
            FROM songs
            WHERE popularity IS NOT NULL
              AND acousticness IS NOT NULL
              AND danceability IS NOT NULL
              AND energy IS NOT NULL
        """

        # Execute the SQL query
        df: DataFrame = self.spark.sql(query)
        
        # Create a new column with the rank with SQL, Grouping in ne next clusters
        # 0-19, 20-39, 40-59, 60-79, 80-100. If the popularity is 100, add it to the cluster 80-100
        df.createOrReplaceTempView("songs")
        query = """
            SELECT *,
                CASE 
                    WHEN popularity = 100 THEN 80
                    ELSE CAST(popularity / 20 AS INT) * 20
                END AS rank
            FROM songs
        """
        df = self.spark.sql(query)

        # Create a new column with the popularity cluster.
        # rank + 19. But if the rank is 80, add 20 to the rank
        df.createOrReplaceTempView("songs")
        query = """
            SELECT 
                popularity,
                acousticness,
                danceability,
                energy,
                CASE 
                    WHEN rank = 80 THEN CONCAT(rank, '-', rank + 20)
                    ELSE CONCAT(rank, '-', rank + 19)
                END AS rank
            FROM songs
        """
        df = self.spark.sql(query)

        # Create a new column with the max value of the row. Write the name of the column in the new column
        df.createOrReplaceTempView("songs")
        query = """
            SELECT *,
                CASE 
                    WHEN acousticness = GREATEST(acousticness, danceability, energy) THEN 'acousticness'
                    WHEN danceability = GREATEST(acousticness, danceability, energy) THEN 'danceability'
                    WHEN energy = GREATEST(acousticness, danceability, energy) THEN 'energy'
                END AS max_value
            FROM songs
        """
        df = self.spark.sql(query)

        # Group by rank and max_value and count the number of rows in each group
        df.createOrReplaceTempView("songs")
        query = """
            SELECT 
                rank,
                max_value,
                COUNT(*) AS count
            FROM songs
            GROUP BY rank, max_value
            ORDER BY rank, max_value
        """
        df = self.spark.sql(query)

        # Calculate the percentage of each group
        df.createOrReplaceTempView("songs")
        query = """
            SELECT 
                rank,
                max_value,
                count,
                (count / SUM(count) OVER (PARTITION BY rank)) * 100 AS percentage
            FROM songs
        """
        df = self.spark.sql(query)

        # Show the result
        df.show()

        # Save the result to a CSV file
        df.write.mode("overwrite").option("header", "true").csv("out/relation_popularity-sql")
        print(f"\033[92;1mCSV file saved\033[0m")
        # Plot the result
        df_pandas: pd.DataFrame = df.toPandas()
        df_pandas["rank"] = df_pandas["rank"].astype(str)
        df_pandas["max_value"] = df_pandas["max_value"].astype(str)
        df_pandas["percentage"] = df_pandas["percentage"].astype(float)
        df_pandas["rank"] = df_pandas["rank"].str.replace("-", " to ")
        df_pandas["max_value"] = df_pandas["max_value"].str.replace("acousticness", "Acousticness")
        df_pandas["max_value"] = df_pandas["max_value"].str.replace("danceability", "Danceability")
        df_pandas["max_value"] = df_pandas["max_value"].str.replace("energy", "Energy")
        # Plot the result as x: rank, y: percentage, hue: max_value
        # 3 lines (one for each max_value)
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=df_pandas, x="rank", y="percentage", hue="max_value", palette="Set1")
        plt.title("Popularity of each attribute")
        plt.xlabel("Popularity")
        plt.ylabel("Percentage")
        plt.legend(title="Attribute")
        plt.savefig("out/relation_popularity-sql.png")
        print(f"\033[92;1mImage saved\033[0m")


    def stop_session(self):
        """
        Stop the Spark session.
        """
        self.spark.stop()
        print(f"\033[92;1mSpark session stopped\033[0m")