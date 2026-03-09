from os import environ as env
from pyspark.sql import SparkSession
import sys

def main():
    # Inicializar Spark Session
    spark = SparkSession.builder \
        .appName("CSV_to_MinIO") \
        .getOrCreate()
    
    # Configurar la conexión S3A apuntando a MinIO local
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", env["MINIO_ROOT_USER"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", env["MINIO_ROOT_PASSWORD"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    # Rutas
    input_path = "./include/data.csv"  # Ruta dentro del contenedor de Airflow
    output_path = "s3a://raw-data/output_parquet/"

    try:
        # Leer el CSV
        print(f"Leyendo datos desde {input_path}")
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df.show(5)  # Mostrar las primeras filas para verificar la lectura

        # Guardar en MinIO
        print(f"Escribiendo datos a {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        print("Proceso finalizado con éxito.")
        # print("Escribiendo datos en la base de datos.")
        # df.write.format("jdbc") \
        #     .option("url", f"jdbc:postgresql://postgres-dw:5432/{env['POSTGRES_DB']}") \
        #     .option("dbtable", "invoice_item") \
        #     .option("user", env["POSTGRES_USER"]) \
        #     .option("password", env["POSTGRES_PASSWORD"]) \
        #     .option("driver", "org.postgresql.Driver") \
        #     .mode("overwrite") \
        #     .save()
        # print("Proceso finalizado con éxito.")
        
    except Exception as e:
        print(f"Error procesando los datos: {e}")
        sys.exit(1)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()