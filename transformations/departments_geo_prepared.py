from pyspark import pipelines as dp
import pyspark.sql.functions as fx


@dp.table(comment="Prepared data obtained from the departments_geo_raw table")
def departments_geo_prepared():
  return spark.read.table("departments_geo_raw").withColumnRenamed("COD_DPTO", "department_id").withColumnRenamed("NOM_DPTO", "department").withColumnRenamed("LATITUD", "latitude").withColumnRenamed("LONGITUD", "longitude").select("department_id", "department", "latitude", "longitude")