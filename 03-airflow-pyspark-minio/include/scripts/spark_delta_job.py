from os import environ as env
import sys

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("CSV_to_Delta_MinIO")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", env["MINIO_ROOT_USER"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", env["MINIO_ROOT_PASSWORD"])
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

    input_path = "./include/data.csv"
    output_path = "s3a://raw-data/output_delta/"

    try:
        print(f"Leyendo datos desde {input_path}")
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df.show(5)

        print(f"Escribiendo datos en formato Delta en {output_path}")
        df.write.format("delta").mode("overwrite").save(output_path)

        print("Verificando que los datos se hayan escrito correctamente en formato Delta")
        df_delta = spark.read.format("delta").load(output_path)
        df_delta.show(5)
        print("Proceso finalizado con éxito.")

    except Exception as e:
        print(f"Error procesando los datos: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
