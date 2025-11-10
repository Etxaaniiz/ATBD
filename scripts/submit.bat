@echo off
chcp 65001 >nul
echo 🚀 Ejecutando análisis en Spark...
docker exec -it spark-master bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/netflix_analysis.py"
pause
