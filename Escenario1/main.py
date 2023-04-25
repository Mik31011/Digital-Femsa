import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sucursales import *

# 1. Extracción y Validación de Datos
def extract_data_from_source(jdbc_url):
    # Configurar SparkSession
    spark = SparkSession.builder \
        .appName("DataConsolidation") \
        .getOrCreate()

    # Leer datos desde la base de datos relacional
    properties = {"user": "username", "password": "password", "driver": "org.postgresql.Driver"}

    df = spark.read \
        .jdbc(url=jdbc_url, table="sales_data", properties=properties)

    # Validar calidad y consistencia de los datos
    df = df.dropna(subset=["column1", "column2"])
    df = df.dropDuplicates(["column1", "column2"])

    return df

# 2. Almacenamiento Centralizado y Seguridad de Datos
def write_data_to_central_storage(df, point_of_sale, partition_date):
    # Escribir DataFrame en el almacén de datos centralizado (Amazon Redshift)
    redshift_url = "jdbc:redshift://redshift-cluster-1.xxxxxxxxxx.us-west-2.redshift.amazonaws.com:5439/dev"
    properties = {"user": "username", "password": "password", "driver": "com.amazon.redshift.jdbc.Driver"}

    df = df.withColumn("point_of_sale", lit(point_of_sale)) \
            .withColumn("partition_date", lit(partition_date))

    df.write \
        .jdbc(url=redshift_url, table="sales_data_consolidated", mode="append", properties=properties)

# 3. Procesamiento y Transformación de Datos
def process_and_transform_data(df):
    # Calcular métricas clave e identificar tendencias
    result = df.groupBy("category").agg(sum("sales_amount").alias("total_sales"))

    return result

# 4. Preparación de Datos y Auditoría
def prepare_data_for_visualization(result, point_of_sale, partition_date):
    # Escribir resultados en el almacén de datos centralizado
    write_data_to_central_storage(result, point_of_sale, partition_date)

def process_branch_data(branch_info):
    jdbc_url = f"jdbc:postgresql://{branch_info['ip']}:5432/mydb"
    df = extract_data_from_source(jdbc_url)
    result = process_and_transform_data(df)
    partition_date = datetime.now().strftime('%Y-%m-%d')
    prepare_data_for_visualization(result, branch_info['punto_venta'], partition_date)

def process_all_branches_concurrently(branches_info):
    with ThreadPoolExecutor() as executor:
        executor.map(process_branch_data, branches_info)

# Tiempo de espera entre ejecuciones (1 hora)
wait_time_seconds = 3600


if __name__ == "__main__":
    try:
        while True:
            process_all_branches_concurrently(branches_info)
            time.sleep(wait_time_seconds)
    except KeyboardInterrupt:
        print("Proceso detenido por el usuario")
    except Exception as e:
        print("Un error ha ocurrido", e)

