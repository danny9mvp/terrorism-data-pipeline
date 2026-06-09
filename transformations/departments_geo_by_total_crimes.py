from pyspark import pipelines as dp

# Please edit the sample below


@dp.materialized_view
def departments_geo_by_total_crimes():
   crimes_prepared_df = spark.read.table("crimes_prepared").select("department_id", "crime_count", "date")
    
   departments_geo_prepared = spark.read.table("departments_geo_prepared").select("department_id", "department", "latitude", "longitude")

   return crimes_prepared_df.join(departments_geo_prepared, "department_id", how="inner").select(
       "department", "latitude", "longitude", "crime_count", "date"
    )
