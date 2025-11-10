from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NetflixAnalysis").getOrCreate()

# Leer el CSV desde la carpeta local compartida (docker-compose monta ./data en /data)
df = spark.read.option("header", "true").csv("/data/netflix_titles_nov_2019.csv")

print("📊 Estructura del dataset:")
df.printSchema()

# Mostrar las primeras filas
df.show(5)

# Contar el número de títulos por país
print("\n🌍 Top 10 países con más títulos:")
df.groupBy("country").count().orderBy("count", ascending=False).show(10)

# Contar películas vs series
print("\n🎥 Distribución de tipo:")
df.groupBy("type").count().show()

spark.stop()