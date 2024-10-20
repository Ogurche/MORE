from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

level = "2" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result" #Имя вашего бакета
your_access_key = os.environ.get('ACCESS') #Ключ от вашего бакета
your_secret_key = os.environ.get('SECRET') #Ключ от вашего бакета

configs = {
    "spark.sql.files.maxPartitionBytes": "1073741824", #1GB
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.block.size": "134217728", # 128MB
    "spark.hadoop.fs.s3a.multipart.size": "268435456", # 256MB
    "spark.hadoop.fs.s3a.multipart.threshold": "536870912", # 512MB
    "spark.hadoop.fs.s3a.committer.name": "magic",
    "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",
    "spark.hadoop.fs.s3a.threads.max": "64",
    "spark.hadoop.fs.s3a.connection.maximum": "64",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "array",
    "spark.hadoop.fs.s3a.directory.marker.retention": "keep",
    "spark.hadoop.fs.s3a.endpoint": "api.s3.az1.t1.cloud",
    "spark.hadoop.fs.s3a.bucket.source-data.access.key": "P2EGND58XBW5ASXMYLLK",
    "spark.hadoop.fs.s3a.bucket.source-data.secret.key": "IDkOoR8KKmCuXc9eLAnBFYDLLuJ3NcCAkGFghCJI",
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.access.key": your_access_key,
    f"spark.hadoop.fs.s3a.bucket.{your_bucket_name}.secret.key": your_secret_key,
    "spark.sql.parquet.compression.codec": "zstd"
}
conf = SparkConf()
conf.setAll(configs.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
log = spark._jvm.org.apache.log4j.LogManager.getLogger(">>> App")

incr_bucket = f"s3a://source-data"
your_bucket = f"s3a://{your_bucket_name}"
incr_table = f"{incr_bucket}/incr{level}"  # таблица с источника ODS , куда мы скопировали инкремент
init_table = f"{your_bucket}/init{level}"  # реплика
temp_table = f"{your_bucket}/temp{level}"

tgt0 = spark.read.parquet(init_table)
columns = tgt0.columns
logging.info("Вычитал реплику")


src0 = spark.read.parquet(incr_table)
logging.info("Вычитал инкремент")

src = src0. \
    withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType())). \
    withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType())). \
    repartition("eff_to_month", "eff_from_month"). \
    selectExpr(columns)
src.cache()
logging.info("Кэшировал инкремент и добавил колонки")
log.info(f"SRC STARTED")

# Записываем инкремент и оставляем только нужные нам закрытые записи и минимальные даты
src.write.mode("overwrite").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
logging.info("Записал инкремент во времянку с партициями по eff_to_month, eff_from_month")
log.info(f"SRC written, defining closed data")
src_closed = src.filter(col("eff_to_month") != lit("5999-12-31")).select("id", "eff_from_month", "eff_from_dt").cache()
logging.info("Создаем набор где eff_to_month НЕ равен строке 5999-12-31")
src_closed.isEmpty()
src.unpersist()

log.info(f"SRC FINISHED")

# Получаем список субпартиций в 5999
rows = tgt0.filter(col("eff_to_month") == lit("5999-12-31")).select(col("eff_from_month").cast(StringType())).distinct().orderBy("eff_from_month").collect()
logging.info("Получаем список субпартиций в 5999")
partitions = [str(row[0]) for row in rows]
n = 0

# Обрабатываем каждую партицию отдельно
for from_dt in partitions:
    n += 1
    log.info(f"{from_dt} STARTED")
    
    # Фильтруем до джойна, так как мы джойним ровно одну субпартицию
    tgt = tgt0.filter(col("eff_to_month") == lit("5999-12-31")).filter(col("eff_from_month") == from_dt)
    src_closed_single_part = src_closed.filter(col("eff_from_month") == from_dt)
    logging.info("Фильтруем до джойна, так как мы джойним ровно одну субпартицию")

    # Убираем все записи, по которым пришли апдейты
    tgt_no_match = tgt.join(src_closed_single_part, on=["id", "eff_from_dt"], how="left_anti")
    logging.info(f"left_anti JOIN -- проход цикла:{n}")

    # Дозаписываем данные в таблицу
    tgt_no_match.write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
    logging.info("Дозаписываем данные в таблицу Temp") 

    log.info(f"{from_dt} FINISHED")

log.info("Moving temp data")

# Теперь надо перенести данные из темповой в основную таблицу
# Закрытые партиции мы переносим просто так, а 5999-12-31 надо перетереть

hadoop_conf = sc._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(your_bucket), hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(f"{init_table}/eff_to_month=5999-12-31/")
fs.delete(path, True)
logging.info("Удаление актуальных записей в реплике") 

spark.read.parquet(temp_table).write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(init_table)
logging.info("Запись в реплику из TMP") 

path = spark._jvm.org.apache.hadoop.fs.Path(temp_table)
fs.delete(path, True)

log.info("Finished")
