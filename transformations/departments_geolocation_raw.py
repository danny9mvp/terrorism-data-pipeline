from pyspark import pipelines as dp
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

from utils.datos_abiertos_client import RestClient
from utils.file_utils import FileUtils
from utils.file_service import FileService

file_path = f"{spark.conf.get("divipola.file.path")}"
divipola_url = f"{spark.conf.get("resources.url")}{spark.conf.get("divipola.url.file.name")}"

client = RestClient()
file_utils = FileUtils()
file_service = FileService(client, file_utils)

schema = StructType([
    StructField("COD_DPTO", IntegerType(), True),
    StructField("NOM_DPTO", StringType(), True),
    StructField("LATITUD", FloatType(), True),
    StructField("LONGITUD", FloatType(), True)
])


@dp.table(comment="Raw data of the colombian department coordinates")
def departments_geo_raw():
    file_service.write_csv_file(divipola_url, file_path)
    return spark.read.csv(file_path, header=True, schema=schema)    
