from pyspark import pipelines as dp
from pyspark.sql.functions import col, rank, sum
from pyspark.sql.window import Window

@dp.materialized_view
def top_departments_by_total_crimes():
    df_total_crimes_by_dept = (
        spark.read.table("crimes_prepared")
        .groupBy(["department", "date"])
        .agg(
            sum("crime_count").alias("total_crimes")
        )
    )
    window_spec = Window.orderBy(col("total_crimes").desc())
    df_top_departments = df_total_crimes_by_dept.withColumn(
        "rank",
        rank().over(window_spec)
    )
    return df_top_departments