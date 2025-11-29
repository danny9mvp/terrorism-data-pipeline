from pyspark import pipelines as dp
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType
from utils.datos_abiertos_file import get_divipola_file

#file_path = f"/Volumes/workspace/default/datasets-storage/DIVIPOLA-_Códigos_departamentos_geolocalizado_20251112.csv"

schema = StructType([
    StructField("COD_DPTO", IntegerType(), True),
    StructField("NOM_DPTO", StringType(), True),
    StructField("LATITUD", FloatType(), True),
    StructField("LONGITUD", FloatType(), True)
])


@dp.table(comment="Raw data of the colombian department coordinates")
def departments_geo_raw():
    divipola_file_content = get_divipola_file()
    return spark.read.csv(file_path, header=True, schema=schema)    
