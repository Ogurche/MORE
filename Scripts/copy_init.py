from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os
from dotenv import load_dotenv

load_dotenv()

level = "2" #Какую таблицу тестируем, маленькую, среднюю или большую
your_bucket_name = "result" #Имя вашего бакета
your_access_key = os.getenv('ACCESS') #Ключ от вашего бакета
your_secret_key = os.getenv('SECRET') #Ключ от вашего бакета


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

table_to_copy = f"init{level}"

src_bucket = f"s3a://source-data"
tgt_bucket = f"s3a://{your_bucket_name}"
src_init_table = f"{src_bucket}/{table_to_copy}"
tgt_init_table = f"{tgt_bucket}/{table_to_copy}"

#hadoop_conf = sc._jsc.hadoopConfiguration()
#src_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(src_bucket), hadoop_conf)
#src_path = spark._jvm.org.apache.hadoop.fs.Path(src_init_table)
#tgt_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(tgt_bucket), hadoop_conf)
#tgt_path = spark._jvm.org.apache.hadoop.fs.Path(tgt_init_table)
#tgt_fs.delete(tgt_path, True)
#spark._jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src_path, tgt_fs, tgt_path, False, hadoop_conf)

spark.read.parquet(src_init_table).write.mode("overwrite").partitionBy("eff_to_month", "eff_from_month").parquet(tgt_init_table)

log.info("Finished")
