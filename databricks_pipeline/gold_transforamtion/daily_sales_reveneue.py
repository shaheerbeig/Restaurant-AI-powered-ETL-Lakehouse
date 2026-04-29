# This file is responsible for transforming the data from the silver layer to the gold layer in the databricks unity catalog.
# Here we are doing some basic transformations like aggregating the data by order date and calculating the 
# total revenue, average revenue, unique customers, unique restaurants and also the count of orders by order type for each day.
from pyspark.sql.functions import *
from pyspark import pipelines as p

@p.materialized_view(name="gold_schema.sales_summary",partition_cols=["order_date"],table_properties={"quality":"gold"})
def sales_summary():
  df_data = (
      p.read("silver_schema.fact_orders").groupBy("order_date")
      .agg(
          countDistinct("order_id").alias("total_orders"),
          sum(col("total_amount")).cast("decimal(10,2)").alias("total_revenue"),
          avg("total_amount").cast("decimal(10,2)").alias("average_total_revenue"),
          countDistinct("customer_id").alias("unique_customers"),
          countDistinct("restaurant_id").alias("unique_restaurant"), 
          countDistinct(
              when(col("order_type") == "delivery", col("order_id"))).alias("delivery_orders"),
          countDistinct(
              when(col("order_type") == "takeaway", col("order_id"))).alias("takeaway_orders"),
          countDistinct(
              when(col("order_type") == "dine_in", col("order_id"))).alias("dine_in_orders"),
      )
      .select ("order_date","total_orders","total_revenue","average_total_revenue","unique_customers",
      "unique_restaurant","delivery_orders","takeaway_orders","dine_in_orders")
  )
  return df_data
