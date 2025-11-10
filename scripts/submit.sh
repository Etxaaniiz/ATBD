#!/bin/bash
echo "🚀 Ejecutando análisis en Spark..."
docker exec -it spark-master python3 /opt/spark-apps/netflix_analysis.py
