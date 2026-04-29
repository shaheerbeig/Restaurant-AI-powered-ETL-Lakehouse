# This file is responsible for ingesting data from eventsHub into the databricks unity catalog.
# Define the credentials in the settings in the databricks workspace and then use those credentials
#  to read the data from the event hub and write it to the bronze layer in the databricks unity catalog.
from pyspark.sql.types import *
from pyspark import pipelines as p
from pyspark.sql.functions import *

EH_NAMESPACE= spark.conf.get("EH_NAMESPACE")
EH_NAME=spark.conf.get("EH_NAME")
EH_CONN_STR=spark.conf.get("EH_CONN_STR")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "30000",
  "maxOffsetsPerTrigger"     : "50000",
  "failOnDataLoss"           : "true",
  "startingOffsets"          : "earliest"
}
@p.table(name="orders",table_properties={"quality":"bronze"})
def orders():
    df_raw_data = (
        spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
    )
    orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("items", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    ])
    data_parsed =( df_raw_data
        .withColumn("key_str",col("key").cast("string"))
        .withColumn("value_str",col("value").cast("string"))
        .withColumn("data",from_json("value_str",orders_schema))
        .select("data.*")
        .withColumnRenamed("timestamp", "order_timestamp")
    )
    return data_parsed
