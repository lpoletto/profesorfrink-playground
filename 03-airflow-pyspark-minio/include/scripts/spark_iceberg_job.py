from os import environ as env
import sys

from pyspark.sql import SparkSession


def main():
    table_name = "default.csv_to_iceberg"
    warehouse_path = "s3a://raw-data/warehouse"

    spark = (
        SparkSession.builder
        .appName("CSV_to_Iceberg_MinIO")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", warehouse_path)
        .config("spark.sql.defaultCatalog", "spark_catalog")
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

    try:
        print(f"Leyendo datos desde {input_path}")
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df.show(5)

        print(f"Escribiendo datos en formato Iceberg usando el warehouse {warehouse_path}")
        (
            df.write.format("iceberg")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print("Verificando que los datos se hayan escrito correctamente en formato Iceberg")
        df_iceberg = spark.table(table_name)
        df_iceberg.show(5)
        print("Proceso finalizado con éxito.")

    except Exception as e:
        print(f"Error procesando los datos: {e}")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
