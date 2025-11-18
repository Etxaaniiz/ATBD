from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, split, explode, avg, length, size, when
from pyspark.sql.functions import col, max as spark_max

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

# ----------------------------------------------------------------------
# 🔹 PUNTO 4 - Análisis de datos (10 preguntas analíticas)
# ----------------------------------------------------------------------

# Limpieza de datos: eliminar filas nulas en campos clave
df = df.dropna(subset=["title", "type", "release_year"])

print("\n✅ Dataset limpio para análisis")

# 1️⃣ Evolución del número de títulos por año y tipo
print("\n1️⃣ ¿Cómo ha evolucionado el número de títulos añadidos a Netflix por año y tipo?")
df.groupBy("release_year", "type") \
  .count() \
  .orderBy("release_year") \
  .show(20)

# 2️⃣ Países que producen más contenido y evolución temporal
print("\n2️⃣ ¿Qué países producen más contenido y cómo ha cambiado con el tiempo?")
df.groupBy("country", "release_year") \
  .count() \
  .orderBy(col("count").desc()) \
  .show(20)

# 3️⃣ Géneros más frecuentes en películas y series
print("\n3️⃣ ¿Cuáles son los géneros más frecuentes por tipo?")
df_genres = df.withColumn("genre", explode(split(col("listed_in"), ", ")))
df_genres.groupBy("type", "genre").count().orderBy(col("count").desc()).show(20)

# 4️⃣ Directores con más títulos en Netflix
print("\n4️⃣ ¿Qué directores tienen más títulos en Netflix?")
df.groupBy("director", "type") \
  .count() \
  .orderBy(col("count").desc()) \
  .na.drop(subset=["director"]) \
  .show(20)

# 5️⃣ Duración promedio de las películas según género o rating
print("\n5️⃣ ¿Cuál es la duración promedio de las películas según su género o rating?")
df_duration = df.filter(df.type == "Movie").withColumn("duration_num", split(col("duration"), " ").getItem(0).cast("int"))
df_duration.groupBy("rating").avg("duration_num").orderBy("avg(duration_num)", ascending=False).show(20)

# 6️⃣ Países con más contenido adulto (TV-MA) y familiar (TV-Y, TV-G)
print("\n6️⃣ ¿Qué países concentran más contenido adulto y familiar?")
adult = df.filter(col("rating") == "TV-MA").groupBy("country").count().orderBy(col("count").desc())
family = df.filter(col("rating").isin("TV-Y", "TV-G")).groupBy("country").count().orderBy(col("count").desc())
print("Contenido adulto (TV-MA):")
adult.show(10)
print("Contenido familiar (TV-Y, TV-G):")
family.show(10)

# 7️⃣ Años con más estrenos y géneros dominantes
print("\n7️⃣ ¿En qué años se estrenaron más títulos y qué géneros dominaban?")
df_genres.groupBy("release_year", "genre").count().orderBy(col("count").desc()).show(20)

# 8️⃣ Relación entre número de actores listados y tipo/género
print("\n8️⃣ ¿Existe relación entre el número de actores listados y el tipo de contenido?")
df_cast = df.withColumn("num_cast", size(split(col("cast"), ", ")))
df_cast.groupBy("type").avg("num_cast").orderBy("type").show()

# 9️⃣ Proporción de coproducciones (títulos con varios países)
print("\n9️⃣ ¿Cuál es la proporción de coproducciones por género?")
df_multi_country = df.withColumn("num_countries", size(split(col("country"), ", ")))
df_multi_country.groupBy("type").agg(
    (count(when(col("num_countries") > 1, True)) / count("*") * 100).alias("porcentaje_coproducciones")
).show()

# 10️⃣ Proporción de títulos recientes (últimos 3 años del dataset)


print("\n🔟 ¿Qué proporción del catálogo corresponde a títulos recientes?")
print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
print(df.columns)
print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')

# Convertir release_year a int y eliminar filas no válidas
df_clean = df.withColumn("release_year_int", col("release_year").cast("int")).dropna(subset=["release_year_int"])
# Obtener año máximo correctamente
max_year = df_clean.agg(spark_max("release_year_int")).collect()[0][0]
# Filtrar últimos 3 años
recent = df_clean.filter(col("release_year_int") >= (max_year - 3))
total = df_clean.count()
recent_count = recent.count()

print(f"Títulos recientes (últimos 3 años): {recent_count} de {total} ({recent_count/total*100:.2f}%)")


# ----------------------------------------------------------------------
# Finalizar la sesión
# ----------------------------------------------------------------------
spark.stop()
print("\n✅ Análisis completado correctamente.")
