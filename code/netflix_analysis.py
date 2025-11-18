from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count, split, explode, avg, length, size, when
from pyspark.sql.functions import col, max as spark_max
import pandas as pd

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
top_countries = df.groupBy("country").count().orderBy("count", ascending=False)
top_countries.show(10)

# Contar películas vs series
print("\n🎥 Distribución de tipo de contenido:")
type_dist = df.groupBy("type").count()
type_dist.show()

# ----------------------------------------------------------------------
# 🔹 PUNTO 4 - Análisis de datos (10 preguntas analíticas)
# ----------------------------------------------------------------------

# Limpieza de datos: eliminar filas nulas en campos clave
df = df.dropna(subset=["title", "type", "release_year"])

print("\n✅ Dataset limpio para análisis")

# 1️⃣ Evolución del número de títulos por año y tipo
print("\n ¿Cómo ha evolucionado el número de títulos añadidos a Netflix por año y tipo?")
df1 = df.groupBy("release_year", "type").count().orderBy("release_year")
df1.show(20)

# 2️⃣ Países que producen más contenido y evolución temporal
print("\n ¿Qué países producen más contenido y cómo ha cambiado con el tiempo?")
df2 = df.groupBy("country", "release_year").count().orderBy(col("count").desc())
df2.show(20)

# 3️⃣ Géneros más frecuentes en películas y series
print("\n ¿Cuáles son los géneros más frecuentes por tipo?")
df_genres = df.withColumn("genre", explode(split(col("listed_in"), ", ")))
df3 = df_genres.groupBy("type", "genre").count().orderBy(col("count").desc())
df3.show(20)

# 4️⃣ Directores con más títulos en Netflix
print("\n ¿Qué directores tienen más títulos en Netflix?")
df4 = df.groupBy("director", "type").count().orderBy(col("count").desc()).na.drop(subset=["director"])
df4.show(20)

# 5️⃣ Duración promedio de las películas según género o rating
print("\n ¿Cuál es la duración promedio de las películas según su género o rating?")
df_duration = df.filter(df.type == "Movie").withColumn("duration_num", split(col("duration"), " ").getItem(0).cast("int"))
df5 = df_duration.groupBy("rating").avg("duration_num").orderBy("avg(duration_num)", ascending=False)
df5.show(20)

# 6️⃣ Países con más contenido adulto (TV-MA) y familiar (TV-Y, TV-G)
print("\n ¿Qué países concentran más contenido adulto y familiar?")
adult = df.filter(col("rating") == "TV-MA").groupBy("country").count().orderBy(col("count").desc())
family = df.filter(col("rating").isin("TV-Y", "TV-G")).groupBy("country").count().orderBy(col("count").desc())

print("Contenido adulto (TV-MA):")
adult.show(10)
print("Contenido familiar (TV-Y, TV-G):")
family.show(10)

# 7️⃣ Años con más estrenos y géneros dominantes
print("\n ¿En qué años se estrenaron más títulos y qué géneros dominaban?")
df7 = df_genres.groupBy("release_year", "genre").count().orderBy(col("count").desc())
df7.show(20)

# 8️⃣ Relación entre número de actores listados y tipo/género
print("\n ¿Existe relación entre el número de actores listados y el tipo de contenido?")
df_cast = df.withColumn("num_cast", size(split(col("cast"), ", ")))
df8 = df_cast.groupBy("type").avg("num_cast").orderBy("type")
df8.show()

# 9️⃣ Proporción de coproducciones (títulos con varios países)
print("\n ¿Cuál es la proporción de coproducciones por género?")
df_multi_country = df.withColumn("num_countries", size(split(col("country"), ", ")))
df9 = df_multi_country.groupBy("type").agg(
    (count(when(col("num_countries") > 1, True)) / count("*") * 100).alias("porcentaje_coproducciones")
)
df9.show()

# 🔟 Proporción de títulos recientes (últimos 3 años del dataset)
print("\n ¿Qué proporción del catálogo corresponde a títulos recientes?")
df_clean = df.withColumn("release_year_int", col("release_year").cast("int")).dropna(subset=["release_year_int"])
max_year = df_clean.agg(spark_max("release_year_int")).collect()[0][0]
recent = df_clean.filter(col("release_year_int") >= (max_year - 3))
total = df_clean.count()
recent_count = recent.count()

print(f"Títulos recientes (últimos 3 años): {recent_count} de {total} ({recent_count/total*100:.2f}%)")

df10 = pd.DataFrame({
    "categoria": ["Recientes", "No recientes"],
    "cantidad": [recent_count, total - recent_count]
})

# ----------------------------------------------------------------------
# 🔹 EXPORTAR RESULTADOS A CSV PARA POWER BI (Punto 5)
# ----------------------------------------------------------------------

df1.toPandas().to_csv("/data/1_titulos_por_anio_tipo.csv", index=False)
df2.toPandas().to_csv("/data/2_paises_contenido_anio.csv", index=False)
df3.toPandas().to_csv("/data/3_generos_frecuentes.csv", index=False)
df4.toPandas().to_csv("/data/4_directores.csv", index=False)
df5.toPandas().to_csv("/data/5_duracion_por_rating.csv", index=False)
adult.toPandas().to_csv("/data/6_contenido_adulto.csv", index=False)
family.toPandas().to_csv("/data/6_contenido_familiar.csv", index=False)
df7.toPandas().to_csv("/data/7_anio_genero.csv", index=False)
df8.toPandas().to_csv("/data/8_cast_por_tipo.csv", index=False)
df9.toPandas().to_csv("/data/9_coproducciones.csv", index=False)
df10.to_csv("/data/10_recientes.csv", index=False)

print("\n CSVs exportados correctamente en /data/ para Power BI")

# ----------------------------------------------------------------------
# Finalizar la sesión
# ----------------------------------------------------------------------
spark.stop()
print("\n✅ Análisis completado correctamente.")
