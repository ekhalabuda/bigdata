# coding: utf-8
import time
import logging
import psutil
import sys
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.storagelevel import StorageLevel 


if len(sys.argv) > 1 and sys.argv[1].lower() == 'true':
    OPTIMIZED = True
    APP_NAME = "AirlineDelaysAnalysisOptimized"
else:
    OPTIMIZED = False
    APP_NAME = "AirlineDelaysAnalysisBase"

# --- Настройка логгера ---
log_format = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)
logger.info(f"Запуск приложения: {APP_NAME}")
logger.info(f"Оптимизация включена: {OPTIMIZED}")

# --- Создание SparkSession с настройками ---
conf = SparkConf()
conf.set("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") # Указываем HDFS
conf.set("spark.executor.memory", "1536m") # Память для executor'ов
conf.set("spark.driver.memory", "2g")   # Память для драйвера
conf.set("spark.driver.maxResultSize", "1g") # На случай больших результатов у драйвера
conf.set("spark.ui.showConsoleProgress", "false") # Отключить прогресс-бар в консоли

# --- Инициализация SparkSession ---
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("spark://spark-master:7077") \
    .config(conf=conf) \
    .getOrCreate()

# Установка уровня логирования Spark (убираем INFO/WARN от самого Spark)
spark.sparkContext.setLogLevel("ERROR")
logger.info("SparkSession создана.")

# --- Замер времени старта ---
start_time = time.time()
process = psutil.Process(os.getpid()) # Получаем текущий процесс для замера RAM

# --- Загрузка данных из HDFS ---
HDFS_PATH = "hdfs:///data/2018_small.csv" # Путь в HDFS
logger.info(f"Загрузка данных из {HDFS_PATH}...")
try:
    df = spark.read.csv(HDFS_PATH, header=True, inferSchema=True)

    # Удаляем ненужный столбец, если он есть
    if "Unnamed: 27" in df.columns:
        df = df.drop("Unnamed: 27")
        logger.info("Столбец 'Unnamed: 27' удален.")

    logger.info("Данные успешно загружены.")
    # Выполним действие (count), чтобы оценить время загрузки и кэширования
    initial_count = df.count()
    logger.info(f"Исходное количество строк: {initial_count}")

except Exception as e:
    logger.error(f"Ошибка при загрузке данных: {str(e)}")
    spark.stop()
    sys.exit(1) # Выход с ошибкой

# --- Предобработка данных ---
logger.info("Начало предобработки данных (fillna, cast)...")
df = df.fillna({
    "DEP_DELAY": 0,
    "ARR_DELAY": 0,
    "CARRIER_DELAY": 0,
    "WEATHER_DELAY": 0,
    "NAS_DELAY": 0,
    "SECURITY_DELAY": 0,
    "LATE_AIRCRAFT_DELAY": 0,
    "AIR_TIME": 0, 
    "ACTUAL_ELAPSED_TIME": 0,
    "CRS_ELAPSED_TIME": 0 
})

numeric_cols_to_cast = [
    "DEP_DELAY", "ARR_DELAY", "DISTANCE", "CRS_ELAPSED_TIME",
    "ACTUAL_ELAPSED_TIME", "AIR_TIME", "CARRIER_DELAY", "WEATHER_DELAY",
    "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
]
for col_name in numeric_cols_to_cast:
     if col_name in df.columns: # Проверяем наличие колонки
        df = df.withColumn(col_name, col(col_name).cast("float"))

logger.info("Предобработка завершена.")

# --- Анализ данных ---
logger.info("Расчет топ-5 авиакомпаний по средней задержке прибытия...")
top_carriers_delay = df.groupBy("OP_CARRIER") \
    .agg(avg("ARR_DELAY").alias("avg_arr_delay")) \
    .orderBy("avg_arr_delay", ascending=False)
top_carriers_delay.show(5)
logger.info("Расчет завершен.")

logger.info("Расчет топ-5 аэропортов отправления по средней задержке вылета...")
top_origin_delay = df.groupBy("ORIGIN") \
    .agg(avg("DEP_DELAY").alias("avg_dep_delay")) \
    .orderBy("avg_dep_delay", ascending=False)
top_origin_delay.show(5)
logger.info("Расчет завершен.")

logger.info("Расчет средних значений по типам задержек...")
delay_columns = ["CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"]
avg_delays = df.agg(*(avg(col(c)).alias(f"avg_{c}") for c in delay_columns))
avg_delays.show()
logger.info("Расчет завершен.")


logger.info("Подготовка признаков для ML...")

# Индексация категориальных признаков
indexer_airline = StringIndexer(inputCol="OP_CARRIER", outputCol="OP_CARRIER_INDEX", handleInvalid="keep") # keep - для обработки новых значений при тесте
indexer_origin = StringIndexer(inputCol="ORIGIN", outputCol="ORIGIN_INDEX", handleInvalid="keep")
indexer_dest = StringIndexer(inputCol="DEST", outputCol="DEST_INDEX", handleInvalid="keep")

# Применяем индексаторы последовательно
pipeline_indexer = Pipeline(stages=[indexer_airline, indexer_origin, indexer_dest])
df = pipeline_indexer.fit(df).transform(df)


# Векторизация признаков
numeric_cols_for_ml = ["DEP_DELAY", "DISTANCE", "CRS_ELAPSED_TIME"] # Выбираем только те, что точно есть и numeric
categorical_cols_indexed = ["OP_CARRIER_INDEX", "ORIGIN_INDEX", "DEST_INDEX"]

assembler = VectorAssembler(
    inputCols=numeric_cols_for_ml + categorical_cols_indexed,
    outputCol="features",
    handleInvalid="skip" 
)
df_assembled = assembler.transform(df)
logger.info("Векторизация признаков завершена.")

# Выбираем только нужные колонки для ML + целевую переменную
ml_df = df_assembled.select("features", "ARR_DELAY")


train_data, test_data = ml_df.randomSplit([0.8, 0.2], seed=42) 
logger.info(f"Данные разделены на обучение ({train_data.count()} строк) и тест ({test_data.count()} строк).")

# --- Оптимизация (Кэширование и Репартиционирование) ---
if OPTIMIZED:
    logger.info("Применение оптимизаций: repartition(8) и cache() для train/test данных...")
    train_data = train_data.repartition(8).cache()
    test_data = test_data.repartition(8).cache()
    # Выполним count, чтобы кэширование сработало до обучения
    train_data.count()
    test_data.count()
    logger.info("Оптимизации применены.")


logger.info("Начало обучения модели LinearRegression...")
lr = LinearRegression(
    featuresCol="features",
    labelCol="ARR_DELAY",
    predictionCol="predicted_delay"
)
model = lr.fit(train_data)
logger.info("Модель обучена.")

# --- Предсказание на тестовых данных ---
logger.info("Получение предсказаний на тестовых данных...")
predictions = model.transform(test_data)
predictions.select("ARR_DELAY", "predicted_delay").show(5)
logger.info("Предсказания получены.")

# --- Оценка модели ---
logger.info("Оценка модели (RMSE)...")
evaluator = RegressionEvaluator(labelCol="ARR_DELAY", metricName="rmse", predictionCol="predicted_delay")
rmse = evaluator.evaluate(predictions)
logger.info(f"Root Mean Squared Error (RMSE) на тестовых данных: {rmse:.3f}")

# --- Очистка кэша
if OPTIMIZED:
    logger.info("Очистка кэша train_data и test_data...")
    train_data.unpersist()
    test_data.unpersist()
    logger.info("Кэш очищен.")

# --- Замер времени и RAM ---
end_time = time.time()
elapsed_time = end_time - start_time
ram_usage_mb = process.memory_info().rss / (1024 * 1024) # RAM в MB

# --- Вывод результатов производительности ---
logger.info("-" * 30)
logger.info(f"РЕЗУЛЬТАТЫ ПРОИЗВОДИТЕЛЬНОСТИ ({'ОПТИМИЗИРОВАНО' if OPTIMIZED else 'БАЗОВО'})")
logger.info(f"Общее время выполнения: {elapsed_time:.2f} секунд")
logger.info(f"Пиковое использование памяти драйвером: {ram_usage_mb:.2f} MB")
logger.info("-" * 30)


# --- Завершение работы Spark ---
logger.info("Завершение Spark-приложения...")
spark.stop()
logger.info("Приложение завершено.")