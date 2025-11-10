@echo off
chcp 65001 >nul
echo 🚀 Ejecutando análisis en Dask...

docker exec -it dask-scheduler bash -c "python /opt/dask-apps/netflix_analysis_dask.py"

pause
