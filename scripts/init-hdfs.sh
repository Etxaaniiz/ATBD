#!/bin/bash
echo "🚀 Inicializando HDFS..."
docker exec -it namenode hdfs dfs -mkdir -p /user/root/input
docker exec -it namenode hdfs dfs -mkdir -p /user/root/output
docker exec -it namenode hdfs dfs -put /data/netflix_titles_nov_2019.csv /user/root/input/
echo "✅ Dataset cargado en HDFS"
