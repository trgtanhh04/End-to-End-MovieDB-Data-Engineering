#!/usr/bin/env python
import os
import logging
import time
from datetime import datetime
import numpy as np
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, when, lit, sum as _sum, regexp_replace, regexp_extract, trim, split, udf, substr, mean, count, desc
from pyspark.sql import functions as F
import psycopg2
from pyspark.sql.functions import explode, split, trim
from dotenv import load_dotenv

# Load biến môi trường
load_dotenv(dotenv_path='/home/tienanh/End-to-End Movie Recommendation/.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PG_USER = os.getenv("PG_USER")  # your username
PG_PASSWORD = os.getenv("PG_PASSWORD")  # your password
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")  # your port
PG_DATABASE = os.getenv("PG_DATABASE")  # your database name

# HDFS_PATH = f"hdfs://localhost:9000/user/hadoop/datalake/{datetime.now().strftime('%Y-%m-%d')}/*/*.json"
HDFS_PATH = f"hdfs://localhost:9000/user/hadoop/datalake/2025-04-10/*/*.json"
CURRENT_DATE_UTC = '2025-04-04 10:58:06'
CURRENT_USER = 'postgres'
PROCESSED_MARKER_DIR = '/home/tienanh/End-to-End Movie Recommendation/marker_files'

def init_spark_sesion():    
    spark = SparkSession.builder \
            .appName("ETL Pipeline: HDFS to PostgreSQL") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .config("spark.jars", "/mnt/e/Downloads/postgresql-42.7.5.jar") \
            .enableHiveSupport() \
            .getOrCreate()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs_default_name = hadoop_conf.get("fs.defaultFS")
    logger.info(f"Hadoop fs.defaultFS configuration: {fs_default_name}")

    hadoop_version = spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
    logger.info(f"Hadoop version: {hadoop_version}")

    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        logger.info(f"FileSystem URI: {fs.getUri()}")
        logger.info("Successfully got filesystem reference")
    except Exception as e:
        logger.error(f"Error accessing HDFS filesystem: {str(e)}")

    logger.info(f"Spark Session initialized with version: {spark.version}")
    return spark

# def convert_runtime_to_float(runtime):
#     try:
#         hours, minutes = map(int, runtime.replace('h', '').replace('m', '').split())
#         return hours + round(minutes / 60, 2)
#     except:
#         return None


# def trans_form(movie_df):
#     movie_df = movie_df.dropDuplicates()
#     movie_df = movie_df.replace(['[]', '-',' -'], None)
    
#     movie_df = movie_df.withColumn(
#         "release_year",
#         F.regexp_extract(F.col("release_year"), r"\d{4}", 0)
#     )

#     movie_df = movie_df.withColumn(
#         "name",
#         F.regexp_replace(F.col("name"), r"\(\d{4}\)$", "")
#     )

#     movie_df = movie_df.withColumn(
#         "genre",
#         F.regexp_replace(F.col("genre"), r"[\[\]']", "")  # bỏ [, ], và dấu nháy đơn '
#     )
    
#     convert_runtime_udf = udf(convert_runtime_to_float, FloatType())
#     movie_df = movie_df.withColumn("runtime_h", convert_runtime_udf(col("runtime"))) \
#                        .drop("runtime")

#     movie_df = movie_df.withColumn("budget_usd", regexp_replace(col("budget"), "[\\$,]", "").cast("float")) \
#                        .withColumn("revenue_usd", regexp_replace(col("revenue"), "[\\$,]", "").cast("float")) \
#                        .drop("budget") \
#                        .drop("revenue")
    
#     return movie_df


# ===================== EXTRACT DATA FROM HDFS =====================

def extract_data_from_hdfs():
    logger.info(f"Extracting data from HDFS path: {HDFS_PATH}")

    spark = init_spark_sesion()

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("release_year", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("runtime", StringType(), True),
        StructField("score", StringType(), True),
        StructField("director", StringType(), True),
        StructField("status", StringType(), True),
        StructField("language", StringType(), True),
        StructField("budget", StringType(), True),
        StructField("revenue", StringType(), True),
        StructField("image", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("metadata", StringType(), True)
    ])

    logger.info(f"Extracting data from HDFS path: {HDFS_PATH}")
    try:
        df = spark.read.option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .schema(schema) \
            .json(HDFS_PATH)
        
        df = df.drop("metadata")
        logger.info(f"Successfully read {df.count()} records from {HDFS_PATH}")
        return df
    except Exception as e:
        logger.error(f"Error reading data from HDFS: {str(e)}")
        return None
    
# ===================== TRANSFORM DATA =====================
def convert_runtime_to_float(runtime):
    try:
        hours, minutes = map(int, runtime.replace('h', '').replace('m', '').split())
        return hours + round(minutes / 60, 2)
    except:
        return None


def clean_raw_values(df):
    return df.replace(['[]', '-', ' -'], None).dropDuplicates()


def extract_release_year(df):
    return df.withColumn(
        "release_year",
        F.regexp_extract(F.col("release_year"), r"\d{4}", 0)
    )


def clean_name_column(df):
    return df.withColumn(
        "name",
        F.regexp_replace(F.col("name"), r"\(\d{4}\)$", "")
    )


def clean_genre_column(df):
    return df.withColumn(
        "genre",
        F.regexp_replace(F.col("genre"), r"[\[\]']", "")
    )


def convert_runtime_column(df):
    convert_runtime_udf = udf(convert_runtime_to_float, FloatType())
    return df.withColumn("runtime_h", convert_runtime_udf(col("runtime"))).drop("runtime")


def convert_money_columns(df):
    return df.withColumn("budget_usd", regexp_replace(col("budget"), "[\\$,]", "").cast("float")) \
             .withColumn("revenue_usd", regexp_replace(col("revenue"), "[\\$,]", "").cast("float")) \
             .drop("budget") \
             .drop("revenue")


def fill_missing_strings(df):
    return df.fillna({
        'name': 'N/A',
        'genre': 'N/A',
        'director': 'N/A',
        'status': 'N/A',
        'language': 'N/A',
        'image': 'N/A',
        'overview': 'N/A'
    })


def cast_id_to_int(df):
    return df.withColumn("id", df["id"].cast("int"))


def fill_missing_release_year(df):
    mode_year = df.groupBy("release_year") \
                  .agg(count("release_year").alias("count")) \
                  .orderBy(desc("count")) \
                  .first()[0]
    return df.fillna({'release_year': mode_year})


def fill_missing_numeric_columns(df):
    numeric_cols = ['runtime_h', 'score', 'budget_usd', 'revenue_usd']
    for col_name in numeric_cols:
        mean_value = df.select(mean(col_name)).collect()[0][0]
        df = df.fillna({col_name: mean_value})
    return df


def trans_form(movie_df):
    movie_df = clean_raw_values(movie_df)
    movie_df = extract_release_year(movie_df)
    movie_df = clean_name_column(movie_df)
    movie_df = clean_genre_column(movie_df)
    movie_df = convert_runtime_column(movie_df)
    movie_df = convert_money_columns(movie_df)
    movie_df = fill_missing_strings(movie_df)
    movie_df = cast_id_to_int(movie_df)
    movie_df = fill_missing_release_year(movie_df)
    movie_df = fill_missing_numeric_columns(movie_df)
    return movie_df

# ===================== LOAD DATA TO POSTGRESQL =====================
# Kiểm tra để tránh insert trùng
def get_existing_movie_ids(spark):
    """Get list of movie_ids that already exist in the movie_info table"""
    try:
        jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        properties = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # Đọc các movie_id đã có từ bảng movie_info
        existing_ids_df = spark.read \
            .jdbc(url=jdbc_url,
                  table='(SELECT movie_id FROM movie_info) as existing_movies',
                  properties=properties)

        return existing_ids_df
    except Exception as e:
        logger.warning(f"Error getting existing movie_ids: {str(e)}")
        # Trả về DataFrame rỗng nếu lỗi (vd: bảng chưa tồn tại)
        return spark.createDataFrame([], "movie_id INT")


def create_tables_if_not_exist():
    """Create the normalized tables in PostgreSQL if they don't exist"""
    logger.info("Creating normalized tables schema if they don't exist")

    try:
        # First check if database exists, create if needed
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            # Connect to default postgres database
            dbname="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if our target database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PG_DATABASE}'")
        exists = cursor.fetchone()

        if not exists:
            logger.info(f"Database {PG_DATABASE} does not exist. Creating it.")
            # Close current connections to postgres before creating new DB
            cursor.close()
            conn.close()

            # Connect with autocommit to create database
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname="postgres"
            )
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {PG_DATABASE}")
            cursor.close()
            conn.close()

        # Now connect to our target database and create tables
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Define the schema creation statements
        create_tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS movie_info (
                movie_id INT PRIMARY KEY,
                name VARCHAR(255),
                release_year INT,
                runtime_minutes FLOAT,
                score FLOAT,
                status VARCHAR(50),
                language VARCHAR(50),
                budget FLOAT,
                revenue FLOAT,
                image TEXT,
                overview TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS director_info (
                movie_id INT,
                director_name VARCHAR(100),
                FOREIGN KEY (movie_id) REFERENCES movie_info(movie_id) ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INT,
                genre VARCHAR(100),
                FOREIGN KEY (movie_id) REFERENCES movie_info(movie_id) ON DELETE CASCADE
            );
            """
        ]
        
        for sql in create_tables_sql:
            cursor.execute(sql)

        logger.info("Schema created successfully")

    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def insert_data_to_postgresql(spark, df):
    logger.info("Inserting data into PostgreSQL")
    create_tables_if_not_exist()

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    properties = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    print(f"Connecting to PostgreSQL with JDBC URL: {jdbc_url}")

    try:
        df.createOrReplaceTempView("movies")

        # Get existing movie_ids to filter out duplicates
        existing_ids_df = get_existing_movie_ids(spark)
        existing_ids = [row.movie_id for row in existing_ids_df.collect()]
        logger.info(f"Found {len(existing_ids)} existing movie_ids in database")

        # Filter out duplicates
        if existing_ids:
            df = df.filter(~col("id").isin(existing_ids))
            filtered_count = df.count()
            logger.info(f"After filtering: {filtered_count} new records to insert")

            if filtered_count == 0:
                logger.info("No new records to insert - all movie_ids already exist in database")
                return 0

        # Transform data with default values for NULLs
        movies_info_df = spark.sql("""
            SELECT 
                id AS movie_id,
                name,
                CAST(release_year AS INT) AS release_year,
                COALESCE(CAST(runtime_h AS FLOAT), 0) AS runtime_minutes,
                COALESCE(CAST(score AS FLOAT), 0) AS score,
                status,
                language,
                COALESCE(CAST(budget_usd AS FLOAT), 0) AS budget,
                COALESCE(CAST(revenue_usd AS FLOAT), 0) AS revenue,
                image,
                overview
            FROM movies
        """)

        director_info_df = spark.sql("""
            SELECT 
                id AS movie_id,
                director AS director_name
            FROM movies
            WHERE director IS NOT NULL
        """)

        movie_genres_df = spark.sql("""
            SELECT 
                id AS movie_id,
                genre
            FROM movies
            WHERE genre IS NOT NULL
        """)



        # Tách genre thành list → explode → xóa khoảng trắng
        movie_genres_df = movie_genres_df \
            .withColumn("genre", explode(split("genre", ","))) \
            .withColumn("genre", trim("genre"))

        # Ghi dữ liệu vào PostgreSQL
        
        logger.info("Writing movie_info table")
        movies_info_df.write \
            .jdbc(url=jdbc_url,
                  table="movie_info",
                  mode="append", # option:  append to avoid overwriting
                  properties=properties)
        logger.info("Successfully wrote data to movie_info table.")

        logger.info("Writing director_info table")
        director_info_df.write \
            .jdbc(url=jdbc_url,
                  table="director_info",
                  mode="append",
                  properties=properties)
        logger.info("Successfully wrote data to director_info table.")

        logger.info("Writing movie_genres table")
        movie_genres_df.write \
            .jdbc(url=jdbc_url,
                  table="movie_genres",
                  mode="append",
                  properties=properties)
        logger.info("Successfully wrote data to movie_genres table.")

        # Log ETL execution
        import psycopg2
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_log (
                id SERIAL PRIMARY KEY,
                execution_date DATE,
                num_records_processed INT,
                status VARCHAR(50),
                executed_by VARCHAR(255)
            );
            """
        )

        cursor.execute("""
            INSERT INTO etl_log (execution_date, num_records_processed, status, executed_by)
            VALUES (%s, %s, %s, %s)
        """, (CURRENT_DATE_UTC, movies_info_df.count(), 'SUCCESS', CURRENT_USER))

        cursor.close()
        conn.close()

        logger.info(f"Data loaded successfully to PostgreSQL warehouse - {movies_info_df.count()} new records")
        return movies_info_df.count()

    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        # Log failed ETL execution
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DATABASE
            )
            conn.autocommit = True
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO etl_log (execution_date, num_records_processed, status, executed_by)
                VALUES (%s, %s, %s, %s)
            """, (CURRENT_DATE_UTC, 0, 'FAILED', CURRENT_USER))

            cursor.close()
            conn.close()
        except:
            pass

        raise

def create_processed_marker(records_processed):
    """Create a marker file to indicate successful processing"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    marker_path = os.path.join(PROCESSED_MARKER_DIR, f"etl_complete_{timestamp}.marker")

    with open(marker_path, 'w') as f:
        f.write(f"ETL completed successfully at {datetime.now().isoformat()}\n")
        f.write(f"Records processed: {records_processed}\n")
        f.write(f"Executed by: {CURRENT_USER}\n")

    logger.info(f"Created processed marker file: {marker_path}")
    
def main():
    spark = init_spark_sesion()
    df = extract_data_from_hdfs()
    
    if df is not None:
        logger.info("Dữ liệu ban đầu:")
        df.show(5)

        df_transform = trans_form(df)
        df_transform.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv("file:///home/tienanh/End-to-End Movie Recommendation/data/data_processed.csv")
        logger.info("Dữ liệu sau khi transform:")
        df_transform.show(5)
        df_transform.printSchema()

        try:
            records_processed = insert_data_to_postgresql(spark, df_transform)
            create_processed_marker(records_processed)
        except Exception as e:
            logger.error(f"Lỗi trong quá trình ETL: {str(e)}")
    else:
        logger.error("Không thể hiển thị dữ liệu vì df là None.")


if __name__ == '__main__':
    main()
# python3 scripts/ETL.py