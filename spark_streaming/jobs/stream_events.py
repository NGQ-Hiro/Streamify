import os
from streaming_functions import *
from schema import schema
from pyspark.sql.functions import *

# kafka topics
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

# kafka port
KAFKA_PORT = "29092"

# snowflake tables
LISTEN_EVENTS_TABLE = 'LISTEN_EVENTS'
PAGE_VIEW_EVENTS_TABLE = 'PAGE_VIEW_EVENTS'
AUTH_EVENTS_TABLE = 'AUTH_EVENTS'


# initialize a spark session
spark = getOrCreateSpark()

# create a read stream and process it
listen_events_stream = createKafkReadStream(spark, LISTEN_EVENTS_TOPIC)
listen_events_stream = processStream(listen_events_stream, schema['listen_events'], LISTEN_EVENTS_TOPIC)

page_view_events_stream = createKafkReadStream(spark, PAGE_VIEW_EVENTS_TOPIC)
page_view_events_stream = processStream(page_view_events_stream, schema['page_view_events'], PAGE_VIEW_EVENTS_TOPIC)

auth_events_stream = createKafkReadStream(spark, AUTH_EVENTS_TOPIC)
auth_events_stream = processStream(auth_events_stream, schema['auth_events'], AUTH_EVENTS_TOPIC)

# write the processed stream to snowflake
writeStream(listen_events_stream, LISTEN_EVENTS_TABLE, "/tmp/checkpoints/listen_events")
writeStream(page_view_events_stream, PAGE_VIEW_EVENTS_TABLE, "/tmp/checkpoints/page_view_events")
writeStream(auth_events_stream, AUTH_EVENTS_TABLE, "/tmp/checkpoints/auth_events")
spark.streams.awaitAnyTermination()

# run is spark-master container as:
# spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages net.snowflake:spark-snowflake_2.13:3.1.3,net.snowflake:snowflake-jdbc:3.25.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/bitnami/spark/jobs/stream_events.py
