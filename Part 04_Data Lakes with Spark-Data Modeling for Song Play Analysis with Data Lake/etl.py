import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Description: 
        Create spark session with hadoop-aws package and return spark.

    Returns:
        spark    
    """
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
        .config("spark.hadoop.fs.s3a.fast.upload","true") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: 
        Load song data from S3, process the data into analytic tables using Spark, and load them back into S3.

    Arguments:
        spark: spark session.
        input_data: the root path of source data from S3.
        output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
#     print(song_data)

    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df_song.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    """
    Description: 
        Load log data from S3, process the data into analytic tables using Spark, and load them back into S3.

    Arguments:
        spark: spark session.
        input_data: the root path of source data from S3.
        output_data: the target path of S3 to sava the analytic tables.

    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page=='NextSong')

    # extract columns for users table    
    users_table = df_log.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x) / 1000.0)
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log = df_log.withColumn("datetime", get_datetime(df_log.ts))
    
    df_log.createOrReplaceTempView("df_log")
    
    # extract columns to create time table
    time_table = spark.sql("""
                        SELECT 
                           distinct datetime as start_time,
                           EXTRACT(hour from datetime) AS hour, 
                           EXTRACT(day from datetime) AS day, 
                           EXTRACT(week from datetime) AS week, 
                           EXTRACT(month from datetime) AS month, 
                           EXTRACT(year from datetime) AS year, 
                           EXTRACT(dayofweek from datetime) AS weekday
                        FROM df_log
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    
    song_df.createOrReplaceTempView("song_df")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                            SELECT row_number() over (order by "somerow") as songplay_id,
                                dl.datetime as start_time, 
                                dl.userId as user_id, 
                                dl.level as user_level, 
                                song_df.song_id,
                                song_df.artist_id, 
                                dl.sessionId as session_id, 
                                dl.location as user_location, 
                                dl.userAgent as user_agent,
                                EXTRACT(year from dl.datetime) AS year,
                                EXTRACT(month from dl.datetime) AS month
                            FROM df_log dl
                            JOIN song_df
                            ON dl.artist = song_df.artist_name and dl.song = song_df.title and dl.length = song_df.duration
                            WHERE dl.page='NextSong'
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "songplays.parq")


def main():
    """
    The main function callbacks create_spark_session() and creates the path of input_data and output_data where they are on S3, 
    then call function of process_song_data() and process_log_data() to use them.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://lcw-udacity-data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
