from pyspark import pipelines as dp
import pyspark.sql.functions as fx


@dp.table(comment="Prepared data obtained from the departments_geo_raw table")
def departments_geo_prepared():
  geo_df = spark.read.table("departments_geo_raw").withColumnRenamed("COD_DPTO", "department_id").groupBy("department_id").agg(fx.first("NOM_DPTO").alias("department"),
            fx.first("LATITUD").alias("latitude"),
            fx.first("LONGITUD").alias("longitude"))
  
  return geo_df.select("department_id", "department", "latitude", "longitude")
