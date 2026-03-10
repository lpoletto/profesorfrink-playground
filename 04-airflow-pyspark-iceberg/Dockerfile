# FROM astrocrpublic.azurecr.io/runtime:3.1-13
FROM astrocrpublic.azurecr.io/astronomer/astro-runtime:12.11.0

USER root

# 1. Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# 2. Crear carpeta para dependencias y descargar los JARs desde Maven
RUN mkdir -p /usr/local/spark-jars && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /usr/local/spark-jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /usr/local/spark-jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar -o /usr/local/spark-jars/postgresql-42.6.0.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar -o /usr/local/spark-jars/iceberg-spark-runtime.jar && \
    curl -fSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar -o /usr/local/spark-jars/delta-spark_2.12-3.2.0.jar && \
    curl -fSL https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar -o /usr/local/spark-jars/delta-storage-3.2.0.jar

# 3. Set JAVA_HOME
# (macos)
# ENV JAVA_HOME /usr/lib/jvm/ java-17-openjdk-arm64
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER astro