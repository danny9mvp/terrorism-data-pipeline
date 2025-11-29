from pyspark import pipelines as dp
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)
from utils.datos_abiertos_client import get_file_content
import utils.file_utils as fu

file_path = f"{spark.conf.get("terrorism.file.path")}"
file_url = f"{spark.conf.get("resources.url")}{spark.conf.get("terrorism.url.file.name")}"

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
    terrorism_file_content = get_file_content(file_url)
    fu.write_csv(file_path, terrorism_file_content)
    return spark.read.csv(file_path, header=True, schema=schema)
