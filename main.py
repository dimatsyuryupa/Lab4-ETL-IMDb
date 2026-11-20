import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "imdb-data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")

os.makedirs(RESULTS_DIR, exist_ok=True)

spark_session = (
    SparkSession.builder
    .master("local[*]")
    .appName("IMDb ETL Pipeline")
    .config(conf=SparkConf())
    .getOrCreate()
)

spark_session.sparkContext.setLogLevel("ERROR")

def read_data(spark_session, file_name, schema):
    path = os.path.join(DATA_DIR, file_name)

    df = (
        spark_session.read
        .option("header", True)
        .option("sep", "\t")
        .option("nullValue", "\\N")
        .schema(schema)
        .csv(path)
    )

    return df


def write_data(df, task_name):
    output_dir = os.path.join(RESULTS_DIR, task_name)

    print(f"Збереження результату: {output_dir}")

    (
        df.write
        .option("header", True)
        .option("sep", "\t")
        .mode("overwrite")
        .csv(output_dir)
    )
test_data = [
    (1, "The Matrix", 1999),
    (2, "Inception", 2010),
    (3, "Interstellar", 2014)
]

test_df = spark_session.createDataFrame(
    test_data,
    ["id", "title", "year"]
)

test_df.show(truncate=False)