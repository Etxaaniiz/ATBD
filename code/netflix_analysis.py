from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, split, explode, avg, length, size, when

# Crear sesión de Spark
spark = SparkSession.builder.appName("NetflixAnalysis").getOrCreate()

# Leer el CSV desde la carpeta compartida en el clúster
df = spark.read.option("header", "true").csv("/data/netflix_titles_nov_2019.csv")

print("📊 Estructura del dataset:")
df.printSchema()
df.show(5)

# ----------------------------------------------------------------------
# 🔹 PUNTO 3 - Verificación del clúster y análisis básico
# ----------------------------------------------------------------------

# Contar el número de títulos por país
print("\n🌍 Top 10 países con más títulos:")
df.groupBy("country").count().orderBy("count", ascending=False).show(10)

# Contar películas vs series
print("\n🎥 Distribución de tipo de contenido:")
df.groupBy("type").count().show()

