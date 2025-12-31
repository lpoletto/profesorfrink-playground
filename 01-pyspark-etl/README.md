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
