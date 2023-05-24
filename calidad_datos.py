import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim, lower, when, regexp_replace, isnan, date_format, length
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import re
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


class LeerData():
    def __init__(self, ruta, archivo, lista):
        self.ruta = ruta
        self.archivo = archivo
        self.lista = lista
        self.lista_df = []
    def lectura(self):
        ruta_s3 = 's3://airbnb-nequi/{}/{}.csv'.format(self.ruta, self.ruta)
        print(ruta_s3)
        # Lee el DataFrame desde S3
        data_cruda = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            format="csv",
            connection_options={"paths": [ruta_s3]},
            format_options={"withHeader": True}
        ).toDF()
        #print(data_cruda.head())

        
        for l in self.lista:
            ruta_s3 = '{}'.format(l)
            print(ruta_s3)
        # Lee el DataFrame desde S3
            data_tratada = glueContext.create_dynamic_frame.from_options(
                connection_type="s3",
                format="parquet",
                connection_options={"paths": [ruta_s3]},
                format_options={"withHeader": True}
            ).toDF()
            self.lista_df.append(data_tratada)
            
        data_tratada = self.lista_df[0]
        for df in self.lista_df[1:]:
            data_tratada = data_tratada.union(df)
        return data_cruda, data_tratada
        
    
def validar_dup(data_cruda, data_tratada, key):
    if key == "listings_edit":
        duplicated_count_cruda = data_cruda.groupBy("id").count().filter(col("count") > 1).count()
        duplicated_count_trata = data_cruda.groupBy("id").count().filter(col("count") > 1).count()
        print("el numero de duplicados de la tabla {} cruda es: {}".format(key, duplicated_count_cruda))
        print("el numero de duplicados de la tabla {} trata es: {}".format(key, duplicated_count_trata))
        if(duplicated_count_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0
    if key == "calendar":
        duplicated_count_cruda = data_cruda.groupBy("listing_id", "date").count().filter(col("count") > 1).count()
        duplicated_count_trata = data_cruda.groupBy("listing_id", "date").count().filter(col("count") > 1).count()
        print("el numero de duplicados de la tabla {} cruda es: {}".format(key, duplicated_count_cruda))
        print("el numero de duplicados de la tabla {} trata es: {}".format(key, duplicated_count_trata))
        if(duplicated_count_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0

def validar_null(data_cruda, data_tratada, key):
    if key == "listings_edit":
        registros_nulos_crudo = data_cruda.filter(data_cruda["id"] == "").count()
        registros_nulos_trata = data_tratada.filter(data_tratada["id"] == "").count()
        print("el numero de nulos en el id de la tabla {} cruda es: {}".format(key, registros_nulos_crudo))
        print("el numero de nulos en el id de la tabla {} trata es: {}".format(key, registros_nulos_trata))
        if(registros_nulos_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0
    if key == "calendar":
        registros_nulos_crudo = data_cruda.filter(data_cruda["listing_id"] == "").count()
        registros_nulos_trata = data_tratada.filter(data_tratada["listing_id"] == "").count()
        print("el numero de nulos en el id de la tabla {} cruda es: {}".format(key, registros_nulos_crudo))
        print("el numero de nulos en el id de la tabla {} trata es: {}".format(key, registros_nulos_trata))
        if(registros_nulos_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0
        
def validar_caracteres_especiales(data_cruda, data_tratada, key):
    conteo_trata = 0
    if key == "calendar":
        columnas = ["adjusted_price", "price"]
        for colu in columnas:
            conteo_cruda = data_cruda.filter(col(colu).like("%$%")).count()
            conteo_trata = data_tratada.filter(col(colu).like("%$%")).count() + conteo_trata
            print("el numero de $ que contiene la tabla {} cruda en la columna {} es {}".format(key, colu, conteo_cruda))
            print("el numero de $ que contiene la tabla {} tratada en la columna {} es {}".format(key, colu, conteo_trata))
        if(conteo_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0
        
    if key == "listings_edit":
        columnas = ["name", "host_name", "neighbourhood_group", "neighbourhood", "room_type"]
        for colu in columnas:
            conteo_cruda = data_cruda.filter(col(colu).rlike("[^a-zA-Z0-9 ]")).count()
            conteo_trata = data_tratada.filter(col(colu).rlike("[^a-zA-Z0-9 ]")).count() + conteo_trata
            print("el numero de caracteres especiales que contiene la tabla {} cruda en la columna {} es {}".format(key, colu, conteo_cruda))
            print("el numero de caracteres especiales que contiene la tabla {} tratada en la columna {} es {}".format(key, colu, conteo_trata))
        if(conteo_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0

def validar_mayusculas(data_cruda, data_tratada, key):
    conteo_trata = 0
    if key == "calendar":
        print("esto no se evalua en la tabla calendar")
        print("ok")
        return 0
    if key == "listings_edit":
        columnas = ["name", "host_name", "neighbourhood_group", "neighbourhood", "room_type"]
        for colu in columnas:
            conteo_cruda = data_cruda.filter(col(colu).rlike("[A|B|C|D|E|F|G|H|I|J|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z]")).count()
            conteo_trata = data_tratada.filter(col(colu).rlike("[A|B|C|D|E|F|G|H|IJ|K|L|M|N|O|P|Q|R|S|T|U|V|W|X|Y|Z]")).count() + conteo_trata
            print("el numero de mayusculas que contiene la tabla {} cruda en la columna {} es {}".format(key, colu, conteo_cruda))
            print("el numero de mayusculas que contiene la tabla {} tratada en la columna {} es {}".format(key, colu, conteo_trata))
        if(conteo_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0
        
def validar_minimun_n(data_cruda, data_tratada, key):
    if key == "calendar":
        conteo_cruda = data_cruda.filter(col("minimum_nights") == "").count()
        conteo_trata = data_tratada.filter(col("minimum_nights") == "").count()
        print("el numero de minimum_nights para la tabla calendar cruda en vacio es {}".format(conteo_cruda))
        print("el numero de minimum_nights para la tabla calendar tratada en vacio es {}".format(conteo_trata))
        if(conteo_trata > 0):
            print("alerta")
            return 1
        else:
            print("ok")
            return 0
    if key == "listings_edit":
        print("esto no se evalua en la tabla listing_editda")
        print("ok")
        return 0
    
def conteo_data(data_cruda, data_tratada, key):
    cont_cruda = data_cruda.count()
    cont_trata = data_tratada.count()
    print("el numero de registros para la tabla {} cruda es {}".format(key, cont_cruda))
    print("el numero de registros para la tabla {} tratada es {}".format(key, cont_trata))
    if key == "listings_edit" and (cont_cruda - 1 == cont_trata):
        print("ok")
        return 0
    elif key == "listings_edit" and (cont_cruda - 1 != cont_trata):
        print("alerta")
        return 1
    
    if key == "calendar" and (cont_cruda == cont_trata):
        print("ok")
        return 0
    else:
        print("alerta")
        return 1
    
def validar_formatos(data_cruda, data_tratada, key):
    column_types_cruda = data_cruda.dtypes
    column_types_tratada = data_tratada.dtypes
    column_types_tratada = [column_types_tratada[a + 5] for a in range(len(column_types_cruda))]
    print("los tipos de datos de la tabla {} cruda son".format(key))
    print(column_types_cruda)
    print("los tipos de datos de la tabla {} tratada son".format(key))
    print(column_types_tratada)
    if column_types_cruda == column_types_tratada and key == 'listings_edit':
        print("ok")
        return 0
    elif(column_types_cruda != column_types_tratada and key == 'listings_edit'):
        print("alerta")
        return 1
    if column_types_cruda == column_types_tratada and key == 'calendar':
        print("alerta")
        return 1
    else:
        elementos_no_coinciden = 0
        for a in range(len(column_types_cruda)):
            if(column_types_cruda[a] != column_types_tratada[a]):
                elementos_no_coinciden = elementos_no_coinciden + 1
        print(elementos_no_coinciden)
        if(elementos_no_coinciden == 2 and len(column_types_cruda) == len(column_types_tratada)):
            print("ok")
            return 0
        else:
            print("alerta")
            return 1
def pruebas_unidad(data_cruda, data_tratada, key):
    if key == "listings_edit":
        value_c = data_cruda.select("name").collect()[1][0]
        value_c = value_c.lower()
        value_c = re.sub(r'[^a-zA-Z0-9\s]', '', value_c)
        print(value_c)
        result = data_tratada.filter(data_tratada.name == value_c).select("name").limit(1).collect()
        print(len(result))
        if((len(result) >= 1)):
            print("ok")
            return 0
        else:
            print("alerta")
            return 1
        
    if key == "calendar":
        value_c = data_cruda.select("price").collect()[1][0]
        value_c = re.sub(r'[^a-zA-Z0-9\s]', '', value_c)
        value_c = int(int(value_c) / 100)
        print(int(value_c / 100))
        result = data_tratada.filter(data_tratada.price == value_c).select("price").limit(1).collect()
        print(result)
        if((len(result) >= 1)):
            print("ok")
            return 0
        else:
            print("alerta")
            return 1
        
def num_archivos(key):
    import boto3
    s3 = boto3.client('s3')
    objects = s3.list_objects(Bucket="airbnb-nequi", Prefix="{}_transfo".format(key))
    file_paths = []
    for obj in objects['Contents']:
        file_path = "s3://airbnb-nequi/{}".format(obj['Key'])
        if file_path.find(".parquet") != -1:
            file_paths.append(file_path)
    #print(file_paths)
    return file_paths
    
order = {"listings_edit": "e8027af0-9ff8-4af6-8c60-2a39b95e5372-0_0-34-1502_20230524011459152",
        "calendar": "018d18f7-4081-4f6b-beec-b1fc12473d1a-0_47-99-3077_20230524011546621"}
for key, value in order.items():
    lista = num_archivos(key)
    data_cruda, data_tratada = LeerData(key, value, lista).lectura()
    print(data_cruda.count())
    print(data_tratada.count())
    #control de pipeline
    validador_1 = validar_null(data_cruda, data_tratada, key)
    validador_2 = validar_caracteres_especiales(data_cruda, data_tratada, key)
    validador_3 = validar_mayusculas(data_cruda, data_tratada, key)
    validador_4 = validar_minimun_n(data_cruda, data_tratada, key)
    if(validador_1 + validador_2 == 0):#+ validador_3 + validador_4 == 0):
        print("el pipeline funciono de manera correcta para la base {}".format(key))
    else:
        print("hubo un error en la ejecucion, compruebe el log para la base {}".format(key))
    #calidad de datos
    calidad_1 = validar_dup(data_cruda, data_tratada, key)
    calidad_2 = conteo_data(data_cruda, data_tratada, key)
    calidad_3 = validar_formatos(data_cruda, data_tratada, key)
    if(calidad_1 + calidad_2 + calidad_3 == 0):
        print("hay calidad en los datos de la tabla {}".format(key))
    else:
        print("no hay calidad en los datos de la tabla {}, revisar log".format(key))
   # prueba de unicidad de script
    unicidad = pruebas_unidad(data_cruda, data_tratada, key)
    if(unicidad == 0):
        print("se cumplen las pruebas de unidad para la tabla {}".format(key))
    else:
        print("no se cumplen las pruebas de unidad para la tabla {}, revisar log".format(key))
job.commit()