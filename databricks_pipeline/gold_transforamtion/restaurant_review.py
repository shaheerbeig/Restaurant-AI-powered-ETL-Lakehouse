# This file is responsible for transforming the data from the silver layer to the gold layer in the databricks unity catalog.
# Here we are doing some basic transformations like aggregating the data by restaurant and calculating the 
# total reviews, average rating, count of each rating and also doing some basic sentiment analysis on the reviews for each restaurant.
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(
    name="gold_schema.restaurant_summary",
    table_properties={"quality": "gold"}
)
def restaurant_summary():
    df_restaurant = (
        spark.table("silver_schema.fact_reviews").groupBy("restaurant_id")
        .agg(
            countDistinct("review_id").alias("review_count"),
            avg("rating").alias("avg_rating"),
            count(when(col("rating") == 5, True)).alias("rating_5_count"),
            count(when(col("rating") == 4, True)).alias("rating_4_count"),
            count(when(col("rating") == 3, True)).alias("rating_3_count"),
            count(when(col("rating") == 2, True)).alias("rating_2_count"),
            count(when(col("rating") == 1, True)).alias("rating_1_count"),
            sum(when(col("sentiment") == "positive", 1).otherwise(0)).alias("sentiment_positive_analysis"),
            sum(when(col("sentiment") == "negative", 1).otherwise(0)).alias("sentiment_negative_analysis"),
            sum(when(col("sentiment") == "neutral", 1).otherwise(0)).alias("sentiment_neutral_analysis")
        )
    )
    df_restaurant_dim = spark.table("silver_schema.dim_resturants")
    df_joined_reviews = (
        df_restaurant_dim.join(df_restaurant, on="restaurant_id", how="left")
        .select(
            "restaurant_id",
            "name",
            "city",
            coalesce(col("review_count"), lit(0)).alias("review_count"),
            coalesce(col("avg_rating"), lit(0)).alias("avg_rating"),
            coalesce(col("rating_5_count"), lit(0)).alias("rating_5_count"),
            coalesce(col("rating_4_count"), lit(0)).alias("rating_4_count"),
            coalesce(col("rating_3_count"), lit(0)).alias("rating_3_count"),
            coalesce(col("rating_2_count"), lit(0)).alias("rating_2_count"),
            coalesce(col("rating_1_count"), lit(0)).alias("rating_1_count"),
            coalesce(col("sentiment_positive_analysis"), lit(0)).alias("sentiment_positive_analysis"),
            coalesce(col("sentiment_negative_analysis"), lit(0)).alias("sentiment_negative_analysis"),
            coalesce(col("sentiment_neutral_analysis"), lit(0)).alias("sentiment_neutral_analysis"),
        )
    )
    return df_joined_reviews