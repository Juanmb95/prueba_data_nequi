import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.ml import Pipeline
from pyspark.ml import Transformer
from pyspark.sql import functions as F
from typing import Dict
from pyspark.sql.functions import col, trim, lower, when, concat, regexp_replace, isnan, date_format
from pyspark.sql.types import IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
#sc._conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

class EliminarReg(Transformer):
    def __init__(self, archivo):
        self.archivo = archivo
        super(EliminarReg, self).__init__()
    def _transform(self, df):
        print("eliminando primera columna y dulicados")
        if (self.archivo != "reviews"):
            df = df.drop_duplicates()
        df = df.filter(df[df.columns[0]] != "")
        #print(df.filter(df[df.columns[0]] == "").count())
        return df
        
class AjustarFormato(Transformer):
    def __init__(self, archivo):
        self.archivo = archivo
        super(AjustarFormato, self).__init__()
    def _transform(self, df):
        print("homologar campos en tipo de texto")
        if (self.archivo == "listings_edit"):
            list_colum = ["name", "host_name", "neighbourhood_group", "neighbourhood", "room_type"]
            for colu in list_colum:
                df = df.withColumn(colu, trim(col(colu)))
                df = df.withColumn(colu, regexp_replace(col(colu), "[^a-zA-Z0-9 ]", " "))#eliminar caracteres especiales
                df = df.withColumn(colu, lower(col(colu)))#homologar todos los campos a minusculas
                df = df.withColumn(colu, trim(col(colu)))
        if(self.archivo == "calendar"):
            list_colum = ["adjusted_price", "price"]
            for colu in list_colum:
                df = df.withColumn(colu, regexp_replace(col(colu), "[$]", ""))
            #print(df.filter(df["minimum_nights"] == "").count())
            df = df.withColumn("minimum_nights", when(col("minimum_nights") == "", "1").otherwise(col("minimum_nights")))
        #print(df.filter(df["minimum_nights"] == "").count())
            df = df.withColumn("adjusted_price", (col("adjusted_price").cast(IntegerType())))
            df = df.withColumn("price", (col("price").cast(IntegerType())))
        return df

class LeerData(Transformer):
    def __init__(self, archivo):
        self.archivo = archivo
        super(LeerData, self).__init__()
    def _transform(self, df):
        ruta_s3 = 's3://airbnb-nequi/{}/{}.csv'.format(self.archivo, self.archivo)
        # Lee el DataFrame desde S3
        data = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            format="csv",
            connection_options={"paths": [ruta_s3]},
            format_options={"withHeader": True}
        ).toDF()
        print(data.head())
        return data
        
        
order = ["listings_edit", "calendar"]
for dataf in order:
    data = LeerData(dataf)
    eliminar_reg_instance = EliminarReg(dataf)
    ajustar_formato_instance = AjustarFormato(dataf)
    pipeline = Pipeline(stages=[data, eliminar_reg_instance, ajustar_formato_instance])
    result = pipeline.fit(data).transform(data)
    table_salida = "{}_transfo".format(dataf)
    if(dataf == "listings_edit"):
        hudiOptions: Dict[str, str] = {
        "hoodie.table.name": table_salida,
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "id",
        #"hoodie.datasource.write.partitionpath.field": "listing_id, date",
        #"hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "airbnb_result",
        "hoodie.datasource.hive_sync.table": table_salida,
        #"hoodie.datasource.hive_sync.partition_fields": "<your_partitionkey_field>",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        }
        result.write.format("hudi").mode("append").options(**hudiOptions).save("s3://airbnb-nequi/{}/".format(table_salida))
    if(dataf == "calendar"):
        result = result.withColumn("key",concat(col("listing_id"), col("date")))
        hudiOptions: Dict[str, str] = {
        "hoodie.table.name": table_salida,
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "key",
        "hoodie.datasource.write.precombine.field": "key",
        #"hoodie.datasource.write.partitionpath.field": "listing_id, date",
        #"hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "airbnb_result",
        "hoodie.datasource.hive_sync.table": table_salida,
        #"hoodie.datasource.hive_sync.partition_fields": "<your_partitionkey_field>",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        }