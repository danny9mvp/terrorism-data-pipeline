from pyspark import pipelines as dp
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructType,
    StructField,
)
from utils.datos_abiertos_client import RestClient
from utils.file_utils import FileUtils
from utils.file_service import FileService

file_path = f"{spark.conf.get("terrorism.file.path")}"
terrorism_url = f"{spark.conf.get("resources.url")}{spark.conf.get("terrorism.url.file.name")}?$limit=50000"

client = RestClient()
file_utils = FileUtils()
file_service = FileService(client, file_utils)

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
    file_service.write_csv_file(terrorism_url, file_path)
    return spark.read.csv(file_path, header=True, schema=schema)
