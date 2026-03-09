import logging
import boto3
import os
import time
from os import environ as env
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError, EndpointConnectionError
from pathlib import Path


logger = logging.getLogger(__name__)


class S3Client:
    """Cliente para interactuar con MinIO/S3 compatible storage."""
    
    _s3_client = None
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # segundos

    def __init__(self):
        """
        Inicializa el cliente de S3/MinIO.
        
        Variables de entorno requeridas:
        - MINIO_ENDPOINT: URL del endpoint de MinIO (default: http://minio:9000)
        - MINIO_ROOT_USER: Usuario de MinIO
        - MINIO_ROOT_PASSWORD: Contraseña de MinIO
        - MINIO_REGION: Región de MinIO (default: us-east-1)
        """
        if self._s3_client is None:
            try:
                endpoint_url = env.get("MINIO_ENDPOINT", "http://minio:9000")
                access_key = env.get("MINIO_ROOT_USER")
                secret_key = env.get("MINIO_ROOT_PASSWORD")
                region_name = env.get("MINIO_REGION", "us-east-1")
                
                if not access_key or not secret_key:
                    raise ValueError("MINIO_ROOT_USER y MINIO_ROOT_PASSWORD son requeridos")
                
                logger.info(f"Conectando a MinIO en: {endpoint_url}")
                
                self._s3_client = boto3.client(
                    "s3",
                    endpoint_url=endpoint_url,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    region_name=region_name,
                )
                
                logger.info("Cliente MinIO inicializado correctamente")
            except (NoCredentialsError, PartialCredentialsError) as e:
                logger.error(f"Error de credenciales: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Error al inicializar cliente MinIO: {str(e)}")
                raise

    def create_bucket(self, bucket_name: str) -> None:
        """
        Crea un bucket en MinIO.
        
        Args:
            bucket_name: Nombre del bucket a crear
            
        Raises:
            ClientError: Si hay error al crear el bucket
            ConnectionError: Si no hay conexión con MinIO
        """
        try:
            # Verificar si el bucket ya existe
            response = self._s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' ya existe")
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            
            if error_code == "404":
                try:
                    self._s3_client.create_bucket(Bucket=bucket_name)
                    logger.info(f"Bucket '{bucket_name}' creado exitosamente")
                except ClientError as create_error:
                    logger.error(f"Error al crear bucket '{bucket_name}': {str(create_error)}")
                    raise
            else:
                logger.error(f"Error al verificar bucket '{bucket_name}': {str(e)}")
                raise ConnectionError(f"No se pudo acceder a MinIO: {str(e)}")

    def _retry_operation(self, operation_name: str, operation_func, *args, **kwargs):
        """
        Ejecuta una operación con reintentos automáticos.
        
        Args:
            operation_name: Nombre de la operación para logging
            operation_func: Función a ejecutar
            *args: Argumentos posicionales
            **kwargs: Argumentos nombrados
            
        Returns:
            Resultado de la operación
            
        Raises:
            ConnectionError: Si falla después de los reintentos
        """
        last_error = None
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                logger.info(f"Intento {attempt}/{self.MAX_RETRIES} para {operation_name}")
                return operation_func(*args, **kwargs)
            except (EndpointConnectionError, ConnectionError, TimeoutError) as e:
                last_error = e
                if attempt < self.MAX_RETRIES:
                    logger.warning(
                        f"Error de conexión en {operation_name} (intento {attempt}/{self.MAX_RETRIES}). "
                        f"Reintentando en {self.RETRY_DELAY} segundos..."
                    )
                    time.sleep(self.RETRY_DELAY)
                else:
                    logger.error(f"Falló {operation_name} después de {self.MAX_RETRIES} intentos")
        
        raise ConnectionError(
            f"No se pudo completar {operation_name} después de {self.MAX_RETRIES} intentos: {str(last_error)}"
        )

    def upload_file(self, file_name: str, bucket: str, object_name: str = None) -> None:
        """
        Sube un archivo a un bucket en MinIO con reintentos automáticos.
        
        Args:
            file_name: Ruta del archivo local a subir
            bucket: Nombre del bucket destino
            object_name: Nombre del objeto en MinIO (default: nombre del archivo)
            
        Raises:
            FileNotFoundError: Si el archivo local no existe
            ClientError: Si hay error al subir el archivo
            ConnectionError: Si no hay conexión con MinIO después de reintentos
        """
        try:
            # Verificar que el archivo existe
            if not Path(file_name).exists():
                raise FileNotFoundError(f"El archivo '{file_name}' no existe")
            
            # Si no se especifica object_name, usar el nombre del archivo
            if object_name is None:
                object_name = Path(file_name).name
            
            logger.info(f"Iniciando carga de '{file_name}' a 's3://{bucket}/{object_name}'")
            
            def do_upload():
                self._s3_client.upload_file(file_name, bucket, object_name)
            
            # Ejecutar con reintentos
            self._retry_operation(
                f"upload_file({file_name} -> {bucket}/{object_name})",
                do_upload
            )
            
            logger.info(f"Archivo '{file_name}' subido exitosamente a 's3://{bucket}/{object_name}'")
            
        except FileNotFoundError as e:
            logger.error(f"Archivo no encontrado: {str(e)}")
            raise
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchBucket":
                logger.error(f"El bucket '{bucket}' no existe")
                raise
            elif error_code == "AccessDenied":
                logger.error(f"Permiso denegado al acceder al bucket '{bucket}'")
                raise PermissionError(f"Acceso denegado al bucket '{bucket}'")
            else:
                logger.error(f"Error al subir archivo: {str(e)}")
                raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"Error inesperado al subir archivo: {str(e)}")
            raise ConnectionError(f"Error de conexión con MinIO: {str(e)}")

    def download_file(self, bucket: str, object_name: str, file_name: str) -> None:
        """
        Descarga un archivo de un bucket en MinIO con reintentos automáticos.
        
        Args:
            bucket: Nombre del bucket origen
            object_name: Nombre del objeto en MinIO
            file_name: Ruta donde guardar el archivo localmente
            
        Raises:
            ClientError: Si hay error al descargar el archivo
            PermissionError: Si hay error de permisos
            ConnectionError: Si no hay conexión con MinIO después de reintentos
        """
        try:
            # Crear directorio si no existe
            Path(file_name).parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Descargando 's3://{bucket}/{object_name}' a '{file_name}'")
            
            def do_download():
                self._s3_client.download_file(bucket, object_name, file_name)
            
            # Ejecutar con reintentos
            self._retry_operation(
                f"download_file({bucket}/{object_name} -> {file_name})",
                do_download
            )
            
            logger.info(f"Archivo descargado exitosamente a '{file_name}'")
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchBucket":
                logger.error(f"El bucket '{bucket}' no existe")
                raise
            elif error_code == "NoSuchKey":
                logger.error(f"El objeto '{object_name}' no existe en el bucket '{bucket}'")
                raise
            elif error_code == "AccessDenied":
                logger.error(f"Permiso denegado al acceder al bucket '{bucket}'")
                raise PermissionError(f"Acceso denegado al bucket '{bucket}'")
            else:
                logger.error(f"Error al descargar archivo: {str(e)}")
                raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"Error inesperado al descargar archivo: {str(e)}")
            raise ConnectionError(f"Error de conexión con MinIO: {str(e)}")

    def delete_file(self, bucket: str, object_name: str) -> None:
        """
        Elimina un archivo de un bucket en MinIO con reintentos automáticos.
        
        Args:
            bucket: Nombre del bucket
            object_name: Nombre del objeto a eliminar
            
        Raises:
            ClientError: Si hay error al eliminar el archivo
            PermissionError: Si hay error de permisos
            ConnectionError: Si no hay conexión con MinIO después de reintentos
        """
        try:
            logger.info(f"Eliminando 's3://{bucket}/{object_name}'")
            
            def do_delete():
                self._s3_client.delete_object(Bucket=bucket, Key=object_name)
            
            # Ejecutar con reintentos
            self._retry_operation(
                f"delete_file({bucket}/{object_name})",
                do_delete
            )
            
            logger.info(f"Objeto '{object_name}' eliminado exitosamente del bucket '{bucket}'")
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "NoSuchBucket":
                logger.error(f"El bucket '{bucket}' no existe")
                raise
            elif error_code == "AccessDenied":
                logger.error(f"Permiso denegado al acceder al bucket '{bucket}'")
                raise PermissionError(f"Acceso denegado al bucket '{bucket}'")
            else:
                logger.error(f"Error al eliminar archivo: {str(e)}")
                raise
        except ConnectionError:
            raise
        except Exception as e:
            logger.error(f"Error inesperado al eliminar archivo: {str(e)}")
            raise ConnectionError(f"Error de conexión con MinIO: {str(e)}")