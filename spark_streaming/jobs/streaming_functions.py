from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, from_json, col, month, hour, dayofmonth, col, year, udf


@udf
def string_decode(s: str, encoding='utf-8'):
    if s:
        decoded = (s.encode('latin1')          # To bytes, required by 'unicode-escape'
                     .decode('unicode-escape') # Perform the actual octal/unicode-escape decode
                     .encode('latin1')         # Map back to bytes
                     .decode(encoding)         # Decode original encoding
                  )
        # Remove all backslashes and strip surrounding quotes
        return decoded
    return s


# run on spark-master in docker
def getOrCreateSpark(app_name="default", master="spark://spark-master:7077"):
    """
    Creates or gets a Spark Session

    Parameters:
        app_name : str
            Pass the name of your app
        master : str
            Choosing the Spark master, yarn is the default
    Returns:
        spark: SparkSession
    """
    spark = (SparkSession
             .builder
             .appName(app_name)
             .master(master=master)
             .getOrCreate())

    return spark


# read from source kafka in docker
def createKafkReadStream(spark, topic, starting_offset="earliest"):
    """
    Creates a kafka read stream

    Parameters:
        spark : SparkSession
            A SparkSession object
        topic : str
            Name of the kafka topic
        starting_offset: str
            Starting offset configuration, "earliest" by default 
    Returns:
        read_stream: DataStreamReader
    """

    read_stream = (spark
                   .readStream
                   .format("kafka")
                   .option("kafka.bootstrap.servers", "broker:29092")
                   .option("failOnDataLoss", False)
                   .option("startingOffsets", starting_offset)
                   .option("subscribe", topic)
                   .load())

    return read_stream


def processStream(stream, stream_schema, topic):
    """
    Process stream to fetch on value from the kafka message.
    convert ts to timestamp format and produce year, month, day,
    hour columns
    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
    Returns:
        stream: DataStreamReader
    """

    # read only value from the incoming message and convert the contents
    # inside to the passed schema
    stream = (stream
              .selectExpr("CAST(value AS STRING)")
              .select(
                  from_json(col("value"), stream_schema).alias(
                      "data")
              )
              .select("data.*")
              )

    # Add month, day, hour to split the data into separate directories
    stream = (stream
              .withColumn("ts", (col("ts")/1000).cast("timestamp"))
              .withColumn("year", year(col("ts")))
              .withColumn("month", month(col("ts")))
              .withColumn("day", dayofmonth(col("ts")))
              .withColumn("hour", hour(col("ts")))
              .withColumn("processed_time", current_timestamp())
              )

    if topic in ["listen_events", "page_view_events"]:
        stream = (stream
                .withColumn("song", string_decode("song"))
                .withColumn("artist", string_decode("artist")) 
                )
    return stream


def writeStream(stream, table_name, checkpoint_path, trigger="5 seconds", output_mode="append"):
    """
    Write the stream back to a file store

    Parameters:
        stream : DataStreamReader
            The data stream reader for your stream
        file_format : str
            parquet, csv, orc etc
        storage_path : str
            The file output path
        checkpoint_path : str
            The checkpoint location for spark
        trigger : str
            The trigger interval
        output_mode : str
            append, complete, update
    """
    sfOptions = {
    "sfURL" : "YSG-PI88233.snowflakecomputing.com",
    "sfUser" : "HI196732",
    "sfPassword" : "8wDGncjn",
    "sfDatabase" : "STREAMIFY",
    "sfSchema" : "STAGING",
    "sfWarehouse" : "COMPUTE_WH"
    }

    def save_to_snowflake(batch_df, batch_id):
        (batch_df.write
            .format("snowflake")
            .options(**sfOptions)
            .option("encoding", "UTF-8")
            .option("dbtable", table_name)
            .mode("append")
            .save())

    write_stream = (stream
                    .writeStream
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(processingTime=trigger)
                    .outputMode(output_mode)
                    .foreachBatch(save_to_snowflake)
                    .start())

    return write_stream