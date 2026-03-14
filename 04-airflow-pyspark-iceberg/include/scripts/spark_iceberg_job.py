from pyspark.sql import SparkSession
import sys

def main():
    # Inicializar Spark con Iceberg y el Catálogo JDBC (Postgres)
    spark = SparkSession.builder \
        .appName("Ingesta_Capa_Raw") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres-dw:5432/datawarehouse") \
        .config("spark.sql.catalog.lakehouse.jdbc.user", "dw_user") \
        .config("spark.sql.catalog.lakehouse.jdbc.password", "dw_password") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://f1-data-lake/") \
        .getOrCreate()
    
    # Configuración de S3/MinIO
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "password123")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    try:
        # 1. Leer el CSV (Capa fuente)
        # df = spark.read.csv("/usr/local/airflow/include/sample.csv", header=True, inferSchema=True)
        # df = spark.read.csv("file:///usr/local/airflow/include/sample.csv", header=True, inferSchema=True)
        input_path = "s3a://f1-data-lake/raw/circuits/*.csv"
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df.show(5)  # Mostrar las primeras filas para verificar la lectura
        
        # 2. Crear el esquema 'staging' si no existe
        print("Asegurando que el esquema 'staging' exista en el catálogo...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.staging")
        
        # 3. Guardar como tabla Iceberg en la capa Staging
        print("Ingestando datos en la tabla lakehouse.staging.circuits ...")
        df.writeTo("lakehouse.staging.circuits") \
          .tableProperty("write.format.default", "parquet") \
          .createOrReplace()
        
        print("¡Ingesta a la capa STAGING completada con éxito!")
        
    except Exception as e:
        print(f"Error en el proceso: {e}")
        sys.exit(1)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()