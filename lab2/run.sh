#!/bin/bash


set -e



DATASET_CSV="2018_small.csv"        
SPARK_APP_SCRIPT="app_spark.py" 
HDFS_DATA_DIR="/data"          

# --- Функции для логирования
log() {
    echo "[INFO] $1"
}

error() {
    echo "[ERROR] $1" >&2 # Вывод ошибки в stderr
    exit 1
}


if [ ! -f "$DATASET_CSV" ]; then
    error "Файл данных '$DATASET_CSV' не найден. Поместите его в текущую директорию."
fi

log "--- Шаг 1: Загрузка данных в HDFS ---"
log "Копируем $DATASET_CSV в контейнер namenode..."
docker cp "$DATASET_CSV" namenode:/ || error "Не удалось скопировать $DATASET_CSV в namenode"

log "Проверяем/создаём директорию $HDFS_DATA_DIR в HDFS..."
docker exec namenode hdfs dfs -mkdir -p "$HDFS_DATA_DIR" || error "Не удалось создать директорию $HDFS_DATA_DIR в HDFS"

log "Загружаем $DATASET_CSV в $HDFS_DATA_DIR/$DATASET_CSV HDFS (перезаписываем, если существует)..."
docker exec namenode hdfs dfs -put -f "/$DATASET_CSV" "$HDFS_DATA_DIR/$DATASET_CSV" || error "Не удалось загрузить $DATASET_CSV в HDFS"
log "Данные успешно загружены в HDFS."

log "--- Шаг 2: Подготовка Spark Master ---"
log "Копируем $SPARK_APP_SCRIPT в контейнер spark-master..."
docker cp "$SPARK_APP_SCRIPT" spark-master:/tmp/ || error "Не удалось скопировать $SPARK_APP_SCRIPT в spark-master"

log "Устанавливаем зависимости Python (psutil, numpy) в контейнере spark-master..."
docker exec spark-master /bin/sh -c "apk update && apk add --no-cache python3 py3-pip python3-dev make automake gcc g++ linux-headers" || error "Не удалось обновить пакеты/установить pip и build tools в spark-master"

docker exec spark-master pip3 install psutil || error "Не удалось установить psutil в spark-master"

docker exec spark-master pip3 install numpy || error "Не удалось установить numpy в spark-master" 
log "Зависимости в spark-master установлены."

log "--- Шаг 3: Запуск Spark-приложений ---"


log "Запускаем БАЗОВЫЙ вариант Spark-приложения..."

docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /tmp/"$SPARK_APP_SCRIPT" False || error "Ошибка при выполнении базового варианта spark-submit"
log "Базовый вариант завершен."

log "---" 


log "Запускаем ОПТИМИЗИРОВАННЫЙ вариант Spark-приложения..."

docker exec spark-master /spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /tmp/"$SPARK_APP_SCRIPT" True || error "Ошибка при выполнении оптимизированного варианта spark-submit"
log "Оптимизированный вариант завершен."

log "--- Все эксперименты завершены успешно! ---"
log "Ищите результаты производительности (время, RAM) в логах выше."