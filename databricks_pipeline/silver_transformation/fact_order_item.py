# this file is responsible for transforming the data from the bronze layer into the silver layer in the databricks unity catalog.
# Here we are doing some basic transformations like parsing the items column which is a JSON string into separate columns 
# and also doing some basic data quality checks like checking for null values and also checking for valid payment methods.
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as p

@p.table(name="fact_order_items",table_properties={"quality":"silver"})
@p.expect_all_or_drop({
  "correct_order_id":"order_id is not null",
  "correct_item_id":"item_id is not null",
  "correct_restaurant_id":"restaurant_id is not null",
  "correct_quantity":"quantity > 0",
  "correct_unit_price" : "unit_price > 0",
  "correct_subtotal" : "subtotal > 0",
})
def fact_order_items():
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

    df_fact_order_item = (
        spark.table("bronze_schema.orders")
        .withColumn("order_timestamp",to_timestamp(col("order_timestamp")))
        .withColumn("order_date",to_date(col("order_timestamp")))
        .withColumn("items_parsed",from_json(col("items"),item_parsed_schema))
        .withColumn("item",explode(col("items_parsed")))
        .select(
            "order_id",
            col("item.item_id").alias("item_id"),
            "restaurant_id",
            "order_timestamp",
            "order_date",
            col("item.name").alias("name"),
            col("item.category").alias("category"),
            col("item.quantity").alias("quantity"),
            col("item.unit_price").cast("decimal(10,2)").alias("unit_price"),
            col("item.subtotal").cast("decimal(10,2)").alias("subtotal")
        )
    )
    return df_fact_order_item


