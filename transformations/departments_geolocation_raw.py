from pyspark import pipelines as dp
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType
from utils.datos_abiertos_client import get_file_content
import utils.file_utils as fu

file_path = f"{spark.conf.get("divipola.file.path")}"
file_url = f"{spark.conf.get("resources.url")}{spark.conf.get("divipola.url.file.name")}"

schema = StructType([
    StructField("COD_DPTO", IntegerType(), True),
    StructField("NOM_DPTO", StringType(), True),
    StructField("LATITUD", FloatType(), True),
    StructField("LONGITUD", FloatType(), True)
])


@dp.table(comment="Raw data of the colombian department coordinates")
def departments_geo_raw():
    divipola_file_content = get_file_content(file_url)
    fu.write_csv(file_path, divipola_file_content)
    return spark.read.csv(file_path, header=True, schema=schema)    
