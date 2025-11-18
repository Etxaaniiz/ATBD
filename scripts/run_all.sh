#!/bin/bash
set -euo pipefail

line() { echo "======================================="; }

need_container() {
  local name="$1"
  if ! docker ps --format '{{.Names}}' | grep -qx "$name"; then
    echo "⚠️  El contenedor '$name' no está en ejecución. Intento arrancarlo..."
    docker start "$name" >/dev/null || {
      echo "❌ No se pudo iniciar '$name'. Revisa 'docker compose up -d' y vuelve a intentarlo."
      exit 1
    }
  fi
}

line
echo "🔥 INICIO DEL PROCESAMIENTO COMPLETO"
line
echo

# Asegura que están arriba los servicios necesarios
need_container "spark-master"
need_container "dask-scheduler"

echo "🚀 Lanzando análisis con Apache Spark..."
if docker exec spark-master bash -lc "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/netflix_analysis.py"; then
  echo "✅ Análisis Spark completado"
else
  echo "❌ Error al ejecutar Spark"
  exit 1
fi
echo

echo "⚙️ Lanzando análisis con Dask..."
# (reintenta arrancar por si acaso)
docker start dask-scheduler >/dev/null 2>&1 || true
if docker exec dask-scheduler bash -lc "python /opt/dask-apps/netflix_analysis_dask.py"; then
  echo "✅ Análisis Dask completado"
else
  echo "❌ Error al ejecutar Dask"
  exit 1
fi
echo

line
echo "✅ PROCESAMIENTO FINALIZADO CORRECTAMENTE"
line
