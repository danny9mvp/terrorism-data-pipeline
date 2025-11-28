from pyspark import pipelines as dp
from pyspark.sql.functions import sum

# Please edit the sample below


@dp.materialized_view
def departments_geo_by_total_crimes():
    departments_by_total_crimes_df = spark.read.table("crimes_prepared").groupBy(["department_id", "date"]).agg(
        sum("crime_count").alias("total_crimes")
    )

    return spark.read.table("departments_geo_prepared").join(departments_by_total_crimes_df, "department_id", how="inner").select(
       "department", "latitude", "longitude", "total_crimes", "date"
    )
