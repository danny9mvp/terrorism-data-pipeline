from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, min, year

# Please edit the sample below

"""
SELECT 
    cp.date,
    YEAR(cp.date) AS year,
    SUM(cp.crime_count) OVER (PARTITION BY YEAR(cp.date)) AS total_crimes
  FROM crimes_prepared cp
"""

@dp.materialized_view
def total_crimes_by_year():
    year_window = Window.partitionBy(year(col("date")))
    return spark.read.table("crimes_prepared").withColumn("total_crimes", F.sum(col("crime_count")).over(year_window)).select("date", year(col("date")).alias("year"), "zone", "total_crimes").distinct()