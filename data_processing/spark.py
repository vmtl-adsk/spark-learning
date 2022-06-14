import pyspark.sql as psql
import pyspark.sql.functions as f
from pyspark.sql.functions import col, from_json
from pyspark.streaming.context import StreamingContext

import config

from src.item import Item
from Spark.schema.schema_item import item_schema
from src.cassandra_connection import Cassandra_connection
from src.psycopg2_connection import PostgreConnection


def spark_session():  # Spark Session with Kafka
    return psql \
        .SparkSession \
        .builder \
        .master('local[2]') \
        .appName('KafkaStreaming') \
        .config('spark.driver.host', config.SPARK_DRIVER_HOST) \
        .config('spark.cassandra.connection.host', config.CASSANDRA_HOST) \
        .config('spark.executor.heartbeatInterval', '100000') \
        .config('spark.network.timeout', '20000') \
        .config('spark.jars.packages',
                'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,org.postgresql:postgresql:42.2.20') \
        .getOrCreate()


######### MISC ########
#---------------------#

def read_from_socket():
    # This method create a DF stream from socket
    # and outputs the processed DF to the console
    lines = spark_session() \
        .readStream \
        .format('socket') \
        .options(host=config.SOCKET_HOST, port=config.SOCKETPORT) \
        .load()

    numbers = lines.select(col('value').cast('integer').alias('number'))
    sumDF = numbers.select(f.sum(col('number')).alias('aggr_so_far'))

    sumDF \
        .writeStream \
        .format('console') \
        .outputMode('complete') \
        .trigger(processingTime='2 seconds') \
        .start() \
        .awaitTermination()


def read_discretized_stream():
    # This method reads lines from socket and transforms the data to a discretized stream
    def streaming_context():
        return StreamingContext(spark_session().sparkContext, 2)

    def transform_to_item(streamData):
        item_id = streamData[0]
        item_name = streamData[1]
        description = streamData[2]
        price = streamData[3]
        tax = streamData[4]
        tax_with_price = streamData[5]

        return Item(item_id, item_name, description, price, tax, tax_with_price)

    sc = streaming_context()
    sc.sparkContext.addPyFile('src/item.py')
    lines = sc.socketTextStream(config.SOCKET_HOST, port=config.SOCKETPORT)
    item = lines.map(lambda line: transform_to_item(line.split(':')))

    item.pprint()
    sc.start()
    sc.awaitTermination()


####### POSTGRE #######
#---------------------#


url = f'jdbc:postgresql://{config.POSTGRE_HOST}:{config.POSTGRE_PORT}/{config.POSTGRE_DB_NAME}'


def create_postgre_connection():
    # Create a Connection to database via psygcopy2
    return PostgreConnection(config.POSTGRE_HOST, config.POSTGRE_PORT,
                             config.POSTGRE_DB_NAME, config.POSTGRE_DB_USER_NAME,
                             config.POSTGRE_DB_PWD)


def read_from_JDBC():
    # This method creates a DF from a JDBC table
    return spark_session() \
        .read \
        .format('jdbc') \
        .options(
            url=url,
            user=config.POSTGRE_DB_USER_NAME,
            password=config.POSTGRE_DB_PWD,
            dbtable=config.POSTGRE_TABLENAME,
            driver=config.SPARK_POSTGRE_DRIVER) \
        .load()


def write_to_JDBC(df, db_table: str):
    # This method writes DF to a JDBC table
    df.write \
        .format('jdbc') \
        .options(
            url=url,
            user=config.POSTGRE_DB_USER_NAME,
            password=config.POSTGRE_DB_PWD,
            dbtable=config.POSTGRE_TABLENAME,
            driver=config.SPARK_POSTGRE_DRIVER) \
        .mode('append') \
        .save()


###### CASSANDRA ######
#---------------------#


def read_from_cassandra():
    # Creating a Spark DF from a Cassandra table
    return spark_session() \
        .read \
        .format('org.apache.spark.sql.cassandra') \
        .options(table=config.CASSANDRA_TABLENAME, keyspace=config.CASSANDRA_KEYSPACE) \
        .load()


def write_stream_batch_to_cassandra():
    # Creating a Spark DF from a stream of JSON files
    # and writing the processed stream to a Cassandra table
    def wrtite_to_cassandra(df, epochId):
        df.write \
            .format('org.apache.spark.sql.cassandra') \
            .options(table=config.CASSANDRA_TABLENAME, keyspace=config.CASSANDRA_KEYSPACE) \
            .mode('append') \
            .save()

    ss = spark_session() \
        .readStream \
        .schema(item_schema()) \
        .json('resources/to_cassandra/*') \
        .select(f.col('item_name'), f.col('item_price')) \
        .writeStream \
        .foreachBatch(wrtite_to_cassandra) \
        .outputMode('update') \
        .start() \
        .awaitTermination()


def write_stream_writer_to_cassandra(cassandraSession):
    # THIS METHOD DOESN'T WORK
    # This will add data to a Cassandra table via rows mode from a JSON files stream

    item_stream = spark_session() \
        .readStream \
        .schema(item_schema()) \
        .json('resources/to_cassandra/*') \
        .select(f.col('item_name'), f.col('price')) \


    def forEachWriter():

        def open(partitionId, epochId):
            print('Open connection')
            return True

        def process(row):
            row = item_stream
            cassandraSession.prepare(
                """
                INSERT INTO cars(item_name, price)
                VALUES (%s, %s)
                """,
                (row.select('item_name'), row.select('price'))
            )

        def close(error):
            print('Closing connection')

    item_stream \
        .writeStream \
        .foreach(forEachWriter) \
        .outputMode('update') \
        .start() \
        .awaitTermination()


#### END CASSANDRA ####
#---------------------#

def read_kafka_topic():
    # This method will read data from a Kafka Topic, transfrom the topic's Value column to json
    # And create a new DF based on the json
    # Then will filter the result DF with item_price > 500 and write it to Cassandra table
    kafka_df = spark_session() \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', config.KAFKA_BOOTSTRAP_SERVER) \
        .option('subscribe', config.KAFKA_TOPIC) \
        .load() \
        .select(f.from_json(f.expr('cast(value as string)'), item_schema())
                .alias('item')) \
        .selectExpr(
            'item.item_id as item_id',
            'item.item_name as item_name',
            'item.description as description',
            'item.price as price',
            'item.tax as tax',
            'item.tax_with_price as tax_with_price')

    kafka_df \
        .where(f.col('price') > 500) \
        .writeStream \
        .format('org.apache.spark.sql.cassandra') \
        .options(table=config.CASSANDRA_TABLENAME,
                 keyspace=config.CASSANDRA_KEYSPACE,
                 checkpointLocation='checkpoints/from_cassandra') \
        .outputMode('append') \
        .start() \
        .awaitTermination(20) \



# Establishing cassandra session and creating a key-space and a table
cassandra = Cassandra_connection()
cassandra.create_key_space(config.CASSANDRA_KEYSPACE)
cassandra.create_table(config.CASSANDRA_TABLENAME)

read_kafka_topic()
