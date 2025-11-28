from pyspark import pipelines as dp
from pyspark.sql import functions as fx


# Please edit the sample below


@dp.materialized_view
def total_crimes_daily():
    return spark.read.table("default.crimes_prepared").groupby("date").agg(
        fx.sum("crime_count").alias("total_crimes")
    )