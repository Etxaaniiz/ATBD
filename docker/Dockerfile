# Dockerfile base (Ubúntu o una distribución ligera de Linux)
FROM openjdk:17-jdk-slim

# 1. Variables de Entorno (Asegura las versiones)
ENV HADOOP_VERSION="3.3.6"
ENV SPARK_VERSION="3.5.0"
ENV SCALA_VERSION="2.12"
ENV HADOOP_HOME="/opt/hadoop"
ENV SPARK_HOME="/opt/spark"
ENV PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin"

# 2. Instalación de dependencias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    python3 python3-pip \
    ssh net-tools procps && \
    apt-get clean

# 3. Descarga e instalación de Hadoop
RUN wget -q https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    rm hadoop-$HADOOP_VERSION.tar.gz

# 4. Descarga e instalación de Spark
RUN wget -q https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /opt/ && \
    ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME && \
    rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# 5. Configuración de claves SSH (Permite el inicio de servicios y la comunicación entre nodos)
RUN ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys

# 6. Copia de archivos de configuración y scripts
COPY ./hadoop/* $HADOOP_HOME/etc/hadoop/
COPY ./spark/* $SPARK_HOME/conf/
# Script para iniciar el servicio correcto 
COPY ./scripts/entrypoint.sh /entrypoint.sh  
RUN chmod +x /entrypoint.sh

# 7. Instalación de PySpark (Requerido para la API Dataframe)
RUN pip3 install pyspark

WORKDIR /opt/hadoop

# Punto de entrada para iniciar los servicios (se diferencia según la variable CONTAINER_ROLE)
ENTRYPOINT ["/entrypoint.sh"]