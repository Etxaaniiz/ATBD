@echo off
chcp 65001 >nul
echo ======================================
echo 🔥 INICIO DEL PROCESAMIENTO COMPLETO
echo ======================================
echo.

echo 🚀 Lanzando análisis con Apache Spark...
docker exec -it spark-master bash -c "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/netflix_analysis.py"
if %errorlevel% neq 0 (
    echo ❌ Error al ejecutar Spark
) else (
    echo ✅ Análisis Spark completado
)
echo.

echo ⚙️ Lanzando análisis con Dask...
docker start dask-scheduler >nul
docker exec -it dask-scheduler bash -c "python /opt/dask-apps/netflix_analysis_dask.py"
if %errorlevel% neq 0 (
    echo ❌ Error al ejecutar Dask
) else (
    echo ✅ Análisis Dask completado
)
echo.

echo ======================================
echo ✅ PROCESAMIENTO FINALIZADO CORRECTAMENTE
echo ======================================
pause
