<<<<<<< HEAD
# 3. Infraestructura de Computación

Se ha desplegado un clúster de **Hadoop y Spark** mediante contenedores **Docker**, cumpliendo con los requisitos de tolerancia a fallos y ejecución distribuida.  
El entorno incluye los siguientes nodos:

- **NameNode:** gestiona el espacio de nombres de HDFS.
- **DataNode:** almacena bloques de datos replicados.
- **Spark Master y Worker:** ejecutan tareas de análisis distribuido.
- **Cliente:** nodo desde el cual se envían trabajos Spark.

El clúster permite ejecutar trabajos Spark correctamente y consultar las interfaces web:

- **NameNode:** http://localhost:9870  
- **Spark Master:** http://localhost:8080  

---

## 3.1 Uso de herramientas adicionales (5%)

Como herramienta transversal del **Tema 6**, se ha integrado **Dask**, un framework de procesamiento paralelo que permite ejecutar operaciones distribuidas sobre grandes volúmenes de datos.  
Se han desplegado dos contenedores adicionales (`dask-scheduler` y `dask-worker`) y se ha creado un script `dask_analysis.py` que ejecuta un análisis del dataset de Netflix en paralelo.

**Dashboard Dask:** http://localhost:8787  

| Herramienta | Descripción | Ventajas |
|--------------|--------------|-----------|
| **Spark** | Procesamiento distribuido completo (batch y SQL) | Escalable, robusto |
| **Dask** | Procesamiento paralelo en Python (API tipo Pandas) | Ligero, rápido, simple |

El uso de Dask demuestra el empleo de herramientas transversales vistas en clase, aplicando los conceptos de *procesamiento paralelo, lazy evaluation* y *cluster computing*.
=======
#Proyecto ATBD Netflix

## 📝 Descripción del Proyecto

Este proyecto tiene como objetivo realizar un **Análisis de Títulos de Netflix** utilizando herramientas de Big Data, específicamente **Apache Spark** sobre un entorno distribuido de **Hadoop** (HDFS).

El entorno se orquesta completamente mediante **Docker Compose**, lo que facilita la configuración y el despliegue de los servicios necesarios.

---

## Puesta en Marcha



---

## Estructura del Directorio

| Ruta | Descripción |
| :--- | :--- |
| `docker/` | Contiene los **archivos de configuración** y **Dockerfiles** para construir las imágenes base de Hadoop y Spark. |
| `docker/Dockerfile` | Definición de la imagen base. |
| `docker/hadoop/` | Archivos de configuración específicos de **Hadoop**. |
| `docker/spark/` | Archivos de configuración específicos de **Spark**. |
| `docker-compose.yml` | Archivo maestro para **definir y orquestar** los 5 contenedores del clúster. |
| `scripts/` | Contiene **scripts de utilidad** para la inicialización y ejecución. |
| `scripts/init-hdfs.sh` | Script para **formatear HDFS** y crear directorios de usuario. |
| `scripts/submit.sh` | Script para **ejecutar el trabajo Spark** (`netflix_analysis.py`). |
| `code/` | Contiene el código fuente. |
| `code/netflix_analysis.py` | Código **PySpark** para las analíticas. |
| `data/` | Directorio para alojar los datasets de entrada. |
| `data/netflix_titles_nov_2019.csv` | El **dataset de Netflix** utilizado para el análisis. |

---

## Tecnologías Utilizadas

* **Docker**
* **Docker Compose**
* **Apache Hadoop** (HDFS)
* **Apache Spark**
* **PySpark** (Python)
>>>>>>> d601ace76300a0403cd91557adb0997a7da2546f
