import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data_view")
    
    # extract columns to create songs table
    songs_table = spark.sql("""SELECT DISTINCT 
                                      song_id, title, artist_id,
                                      year, duration
                                 FROM song_data_view
                                WHERE song_id IS NOT NULL 
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT 
                                        artist_id, artist_name as name, artist_location as location, 
                                        artist_latitude as lattitude, artist_longitude as longitude
                                   FROM song_data_view
                                  WHERE artist_id IS NOT NULL 
                              """)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
                        

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data,'logdata/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")
    df.createOrReplaceTempView("log_data_view")
      
    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINCT 
                                      userId as user_id, firstName as first_name, 
                                      lastName as last_name, gender, level
                                 FROM log_data_view
                                WHERE userID IS NOT NULL  
                            """)
     
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')                        

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)), TimestampType())
    df = df.withColumn('date_time', get_datetime(df.ts))
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_data_view")
    time_table = spark.sql("""SELECT DISTINCT 
                                     date_time as start_time, 
                                     hour(date_time) as hour, 
                                     day(date_time) as day, 
                                     weekofyear(date_time) as week, 
                                     month(date_time) as month, 
                                     year(date_time) as year, 
                                     dayofweek(date_time) as weekday
                                FROM log_data_view
                               WHERE date_time IS NOT NULL  
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'times.parquet'), 'overwrite')
                        
    # reading song, artist and time tables to insert songplays table
    song_df = spark.read.parquet("songs.parquet")
    song_df.createOrReplaceTempView("songs_table")

    artist_df = spark.read.parquet("artists.parquet")
    artist_df.createOrReplaceTempView("artists_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                          l.date_time as start_time, year(l.date_time) as year, month(l.date_time) as month, 
                                          l.userId, l.level, l.song, s.song_id, l.artist, a.artist_id, 
                                          l.sessionId, l.location, l.userAgent
                                     FROM log_data_view l
                                LEFT JOIN songs_table s ON (s.title = l.song)
                                LEFT JOIN artists_table a ON (a.name = l.artist)   
                               """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/" 
    output_data = "s3a://udacity-dend/dloutput/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
