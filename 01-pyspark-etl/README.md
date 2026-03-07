Crear el archivo .env dentro de la carpeta del proyecto:
```sh
MSSQL_SA_USER=SA
MSSQL_SA_PASSWORD=StrongPassw0rd123
MSSQL_PORT=1433
MSSQL_DB=AdventureWorksDW2022
MSSQL_HOST=mssql-server

POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres_db
POSTGRES_PORT=5432
POSTGRES_HOST=postgres

SPARK_JAR_PATH="/opt/spark/jars/postgresql-42.5.2.jar"
```

Ir a la carpeta:
```sh
cd spark_jars
```
Descargar los JARs necesarios para conectarse a las DB.

MSSQL Server
```sh
wget -c https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.4.2/mssql-jdbc-12.4.2.jre11
```

PostgreSQL
```sh
wget -c https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.2/postgresql-42.5.2.jar
```

Comprobar archivos descargados:
```sh
ls -lh mssql-jdbc-12.4.2.jre11.jar postgresql-42.5.2.jar
```
