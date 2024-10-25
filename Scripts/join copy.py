from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession , Window 
from pyspark.conf import SparkConf
import os
import logging
import dotenv

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

dotenv.load_dotenv()

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
    "spark.hadoop.fs.s3a.threads.max": "64", # можно увеличить 
    "spark.hadoop.fs.s3a.connection.maximum": "64", # можно увеличить 
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
repl_table = f"{your_bucket}/init{level}"  # реплика
temp_table = f"{your_bucket}/temp{level}"

replica0 = spark.read.parquet(repl_table)
columns = replica0.columns
logging.info("Вычитал реплику")


increment0 = spark.read.parquet(incr_table)
logging.info("Вычитал инкремент")

increment = increment0. \
    withColumn("eff_from_month", last_day(col("eff_from_dt")).cast(StringType())). \
    withColumn("eff_to_month", last_day(col("eff_to_dt")).cast(StringType())). \
    repartition("eff_to_month", "eff_from_month"). \
    selectExpr(columns)
increment.cache()
logging.info("Кэшировал инкремент и добавил колонки")
log.info(f"increment STARTED")

# Записываем записи во времянку
increment.write.mode("overwrite").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
increment.unpersist()

replica = replica0.filter(col("eff_to_month") == lit("5999-12-31")).selectExpr(columns).cache()
replica.write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(temp_table)
replica.unpersist()


# Выбираем по row_number = 1 and 5999
w = Window.partitionBy("id").orderBy(desc('eff_from_dt'),desc('eff_to_dt'))
# to_insert0 = spark.read.parquet(temp_table)
to_insert = spark.read.parquet(temp_table) \
    .repartition('id') \
    .withColumn('rn', row_number().over(w)) \
    .filter(((col("eff_to_dt") == lit("5999-12-31")) & (col('rn') == 1)) | (col("eff_to_dt") != lit("5999-12-31"))) \
    .selectExpr(columns) \
    .cache()


# Теперь надо перенести данные из темповой в основную таблицу
# Закрытые партиции мы переносим просто так, а 5999-12-31 надо перетереть

hadoop_conf = sc._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(your_bucket), hadoop_conf)
path = spark._jvm.org.apache.hadoop.fs.Path(f"{repl_table}/eff_to_month=5999-12-31/")
fs.delete(path, True)
logging.info("Удаление актуальных записей в реплике") 

to_insert.write.mode("append").partitionBy("eff_to_month", "eff_from_month").parquet(repl_table)
to_insert.unpersist()
logging.info("Запись в реплику из TMP") 

path = spark._jvm.org.apache.hadoop.fs.Path(temp_table)
fs.delete(path, True)

log.info("Finished")
