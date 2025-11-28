from pyspark import pipelines as dp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    StructField,
)

file_path = f"/Volumes/workspace/default/datasets-storage/TERRORISMO_20251110.csv"

schema = StructType(
    [
        StructField("FECHA_HECHO", StringType(), True),
        StructField("COD_DEPTO", IntegerType(), True),
        StructField("DEPARTAMENTO", StringType(), True),
        StructField("COD_MUNI", IntegerType(), True),
        StructField("MUNICIPIO", StringType(), True),
        StructField("ZONA", StringType(), True),
        StructField("CANTIDAD", IntegerType(), True),
    ]
)


@dp.table(comment="Raw data of the number of crimes committed in Colombia")
def crimes_raw():
    return spark.read.csv(file_path, header=True, schema=schema)
