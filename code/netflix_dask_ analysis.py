from dask.distributed import Client
import dask.dataframe as dd
import time

# Conectar al scheduler Dask
client = Client("tcp://dask-scheduler:8786")
print("✅ Conectado al clúster Dask")
print(client)

inicio = time.time()

# Leer el CSV desde la carpeta compartida
df = dd.read_csv("/data/netflix_titles_nov_2019.csv")

print("\n📊 Columnas del dataset:")
print(df.columns)

# Total de títulos
total_titulos = df.shape[0].compute()
print(f"\n🎬 Total de títulos: {total_titulos}")

# Top 10 países
print("\n🌍 Top 10 países con más títulos:")
top_paises = df.groupby("country").size().nlargest(10).compute()
print(top_paises)

# Películas vs series
print("\n🎥 Distribución de tipo:")
tipos = df["type"].value_counts().compute()
print(tipos)

fin = time.time()
print(f"\n⏱️ Tiempo total: {fin - inicio:.2f} segundos")

client.close()
