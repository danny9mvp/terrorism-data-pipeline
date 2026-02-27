from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.materialized_view(
    comment="Data of the number of crimes committed in Colombia prepared for analysis"
)
@dp.expect_or_drop("date", "date is not null and date between '2009-01-01' and '2025-12-31'")
@dp.expect("department_id", "department_id is not null")
@dp.expect("department", "department is not null")
@dp.expect("city_id", "city_id is not null")
@dp.expect("city", "city is not null")
@dp.expect("zone", "zone is not null")
@dp.expect("crime_count", "crime_count is not null and crime_count > 0")
def crimes_prepared():
    return (
        spark.read.table("crimes_raw")
        .withColumn("date", to_date(substring("FECHA_HECHO", 0, 10), "yyyy-MM-dd"))
        .withColumnRenamed("COD_DEPTO", "department_id")
        .withColumnRenamed("DEPARTAMENTO", "department")
        .withColumnRenamed("COD_MUNI", "city_id")
        .withColumnRenamed("MUNICIPIO", "city")
        .withColumnRenamed("ZONA", "zone")
        .withColumnRenamed("CANTIDAD", "crime_count")
        .select(
            "date",
            "department_id",
            "department",
            "city_id",
            "city",
            "zone",
            "crime_count",
        )
    )