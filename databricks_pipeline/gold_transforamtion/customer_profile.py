# This file is responsible for transforming the data from the silver layer to the gold layer in the databricks unity catalog.
# Here we are doing some basic transformations like aggregating the data by customer and calculating the
#  total orders, lifetime spend, average order value, loyalty tier, favorite restaurant and favorite item for each customer.
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark import pipelines as dp

@dp.materialized_view(name="gold_schema.customer_360_review",table_properties={"quality":"gold"})
def customer_360_review():

  df_orders = dp.read("silver_schema.fact_orders")
  df_customer_orders = (
    df_orders
    .groupBy("customer_id")
    .agg(
      countDistinct("order_id").alias("total_orders"),
      sum("total_amount").alias("lifetime_spend"),
      avg("total_amount").alias("avg_order_value"),
      max("order_date").alias("last_order_date"),
    )
    .withColumn(
            "loyalty_tier", 
            when(col("total_orders") >= 50, "Platinum")
            .when(col("total_orders") >= 25, "Gold")
            .when(col("total_orders") >= 10, "Silver")
            .otherwise("Bronze")
        )
  )

  df_reviews = dp.read("silver_schema.fact_reviews")
  df_customer_reviews = (
    df_reviews
    .groupBy("customer_id")
    .agg(
      round(avg("rating"),2).alias("avg_rating"),
      countDistinct("review_id").alias("total_reviews")
    )
  )

  df_restaurants = dp.read("silver_schema.dim_resturants")
  df_fav_restaurant = (
    df_orders.join(df_restaurants,on="restaurant_id")
    .groupBy("customer_id","name")
    .agg(count("order_id").alias("total_orders"))
    .withColumn("row_number",row_number().over(Window.partitionBy("customer_id").orderBy(desc("total_orders"))))
    .filter(col("row_number")==1)
    .select("customer_id", col("name").alias("restaurant_name"))
  )

  df_order_items = dp.read("silver_schema.fact_order_items")
  df_fav_item = (
    df_orders.join(df_order_items , on="order_id")
    .groupBy("customer_id","name")
    .agg(
      sum("quantity").alias("item_qty")
    )
    .withColumn("rn",row_number().over(Window.partitionBy("customer_id").orderBy(desc("item_qty"))))
    .filter(col("rn")==1)
    .select("customer_id",col("name").alias("fav_item"))
  )

  df_customers = dp.read("silver_schema.dim_customers").alias("c")
  df_customers_360 = (df_customers.join(df_customer_orders, on="customer_id",how="left")       
                  .join(df_customer_reviews, on="customer_id",how="left")
                  .join(df_fav_restaurant, on="customer_id",how="left")
                  .join(df_fav_item, on="customer_id",how="left")
                  .select(
                    "customer_id",
                    col("c.name").alias("customer_name"),
                    "email",
                    "city",
                    "join_date",
                    "loyalty_tier",
                    coalesce(col("total_orders"),lit(0)).alias("total_orders"),
                    coalesce(col("lifetime_spend"),lit(0)).alias("lifetime_spend"),
                    coalesce(col("avg_order_value"),lit(0)).alias("avg_order_value"),
                    "last_order_date",
                    coalesce(col("avg_rating"),lit(0)).alias("avg_rating_given"),
                    coalesce(col("total_reviews"),lit(0)).alias("total_reviews"),

                    "restaurant_name",
                    "fav_item",
                    when(col("lifetime_spend")>=5000,True).otherwise(False).alias("is_vip")
                  )
                )
  return df_customers_360

