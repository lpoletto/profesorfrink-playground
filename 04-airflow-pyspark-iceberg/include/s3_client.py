import logging
import boto3
from botocore.exceptions import ClientError
from os import environ as env
import os

# Configuración básica de logging para ver los mensajes en consola
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class S3Client:
    """
    Clase cliente para interactuar con Amazon S3 o MinIO usando boto3.
    """
    def __init__(self, access_key: str = env["MINIO_ROOT_USER"], secret_key: str = env["MINIO_ROOT_PASSWORD"], endpoint_url: str = env["MINIO_ENDPOINT"], region_name: str = env["MINIO_REGION"]):
        self.logger = logging.getLogger(__name__)
        self.region_name = region_name
        
        # Inicializamos el cliente de boto3. 
        # Si endpoint_url no es None, boto3 apuntará a ese endpoint (Ej: MinIO)
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=endpoint_url,
                region_name=region_name
            )
            self.logger.info(f"Cliente S3 inicializado correctamente. Endpoint: {endpoint_url or 'AWS Default'}")
        except Exception as e:
            self.logger.error(f"Error al inicializar el cliente de S3: {e}")
            raise

    def _bucket_exists(self, bucket_name: str) -> bool:
        """Método privado auxiliar para verificar si el bucket ya existe y tenemos acceso."""
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            # Si el error es 404, significa que el bucket no existe
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            # Si es otro error (ej. 403 Forbidden), el bucket existe pero no es nuestro, o hay otro problema
            self.logger.error(f"Error verificando la existencia del bucket '{bucket_name}': {e}")
            raise

    def create_bucket(self, bucket_name: str) -> bool:
        """Crea un bucket verificando previamente que no exista."""
        if self._bucket_exists(bucket_name):
            self.logger.info(f"El bucket '{bucket_name}' ya existe. Omitiendo creación.")
            return True

        try:
            # AWS requiere la configuración de LocationConstraint si la región no es us-east-1
            if self.region_name != 'us-east-1':
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': self.region_name}
                )
            else:
                self.s3_client.create_bucket(Bucket=bucket_name)
            self.logger.info(f"Bucket '{bucket_name}' creado exitosamente.")
            return True
        except ClientError as e:
            self.logger.error(f"No se pudo crear el bucket '{bucket_name}': {e}")
            return False

    def upload_file(self, file_name: str, bucket_name: str, object_name= None) -> bool:
        """Sube un archivo al bucket especificado."""
        # Si no se especifica el nombre del objeto en S3, usamos el nombre original del archivo
        if object_name is None:
            object_name = os.path.basename(file_name)

        try:
            self.s3_client.upload_file(file_name, bucket_name, object_name)
            self.logger.info(f"Archivo '{file_name}' subido correctamente a '{bucket_name}/{object_name}'.")
            return True
        except ClientError as e:
            self.logger.error(f"Error subiendo el archivo '{file_name}': {e}")
            return False
        except FileNotFoundError:
            self.logger.error(f"El archivo local '{file_name}' no fue encontrado.")
            return False

    def download_file(self, bucket_name: str, object_name: str, file_name: str) -> bool:
        """Descarga un archivo desde el bucket especificado."""
        try:
            self.s3_client.download_file(bucket_name, object_name, file_name)
            self.logger.info(f"Objeto '{object_name}' descargado correctamente como '{file_name}'.")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.error(f"El objeto '{object_name}' no existe en el bucket '{bucket_name}'.")
            else:
                self.logger.error(f"Error descargando el archivo '{object_name}': {e}")
            return False