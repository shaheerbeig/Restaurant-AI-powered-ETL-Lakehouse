# this file is responsible for transforming the data from the bronze layer into the silver layer in the databricks unity catalog.
# Here we are doing some basic transformations like parsing the items column which is a JSON string into separate columns and 
# also doing some basic data quality checks like checking for null values and also checking for valid payment methods.
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as p

@p.table(name="silver_schema.fact_orders",table_properties={"quality":"silver"})
@p.expect_all_or_drop({
  "correct_order_id":"order_id is not null",
  "correct_customer_id":"customer_id is not null",
  "correct_restaurant_id":"restaurant_id is not null",
  "correct_amount":"total_amount > 0",
  "correct_payment_method" : "payment_method in ('cash','card','wallet')"
})
def fact_orders():
  item_parsed_schema = ArrayType(
      StructType([
          StructField("item_id",StringType()),
          StructField("name",StringType()),
          StructField("category",StringType()),
          StructField("quantity",IntegerType()),
          StructField("unit_price",DecimalType(10,2)),
          StructField("subtotal",DecimalType(10,2)),       
      ])
  )
  df_orders = (
      spark.table("bronze_schema.orders")
      .withColumn("order_timestamp",to_timestamp(col("order_timestamp")))
      .withColumn("order_date",to_date(col("order_timestamp")))
      .withColumn("order_hour",hour(col("order_timestamp")))
      .withColumn("day",date_format(col("order_timestamp"),"EEEE"))
      .withColumn("is_weekend",when(col("day").isin(["Saturday","Sunday"]),True).otherwise(False))
      .withColumn("order_items_parsed",from_json(col("items"),item_parsed_schema))
      .withColumn("item_count",size(col("order_items_parsed")))
      .select(
          "order_id",
          "order_timestamp",
          "order_date",
          "order_hour",
          "day",
          "is_weekend",
          "restaurant_id",
          "customer_id",
          "order_type",
          "item_count",
          col("total_amount").cast("decimal(10,2)").alias("total_amount"),
          "payment_method",
          "order_status"
      )
  )
  return df_orders