# utils.py
import os
import json
from typing import Dict, Any
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from clickhouse_driver import Client


# Load environment variables from .env file (假設 .env 檔案在 connect_config 資料夾中)
dotenv_path = os.path.join(os.path.dirname(__file__),'connect_config', '.env')

load_dotenv(dotenv_path)

CLICKHOUSE_TABLE_NAME = "sensor_data"

def load_config(config_path: str = 'config.json') -> Dict[str, Any]:
    """
    載入設定檔，優先使用環境變數。
    """
    print()
    return {
        'mongodb_uri': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
        'mongodb_db': os.getenv('MONGODB_DB', 'iot'),
        'mongodb_collection': os.getenv('MONGODB_COLLECTION', 'sensor_data'),
        'clickhouse_host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
        'clickhouse_port': os.getenv('CLICKHOUSE_PORT', '8123'),
        'clickhouse_tcp_port': os.getenv('CLICKHOUSE_TCP_PORT', '9000'),
        'clickhouse_user': os.getenv('CLICKHOUSE_USER', 'default'),
        'clickhouse_password': os.getenv('CLICKHOUSE_PASSWORD', 'default'),
        'clickhouse_database': os.getenv('CLICKHOUSE_DATABASE', 'default')
    }



def init_spark(app_name: str, config: Dict[str, Any]) -> SparkSession:
    """
    初始化 Spark Session 並設定 MongoDB 和 ClickHouse 連接器。
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "4g") \
        .config("spark.clickhouse.write.batchSize", str(10000)) \
        .config("spark.clickhouse.write.maxRetry", str(3)) \
        .config("spark.clickhouse.write.retryInterval", "60") \
        .config("spark.clickhouse.write.repartitionByPartition", "true") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1," +
                "com.clickhouse.spark:clickhouse-spark-runtime-3.4_2.12:0.8.0") \
        .config("spark.mongodb.read.connection.uri", f"{config['mongodb_uri']}{config['mongodb_db']}.{config['mongodb_collection']}?authSource=admin") \
        .config("spark.mongodb.write.connection.uri", f"{config['mongodb_uri']}{config['mongodb_db']}.{config['mongodb_collection']}?authSource=admin") \
        .getOrCreate()

def load_from_mongodb(spark: SparkSession, config: Dict[str, Any]):
    """
    從 MongoDB 載入資料到 Spark DataFrame。
    """
    return spark.read.format("mongodb") \
        .option("uri", f"{config['mongodb_uri']}{config['mongodb_db']}.{config['mongodb_collection']}?authSource=admin") \
        .load()

def configure_clickhouse_spark(spark: SparkSession, config: Dict[str, Any]):
    """
    設定 Spark 的 ClickHouse Catalog。
    """
    spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    spark.conf.set("spark.sql.catalog.clickhouse.host", config['clickhouse_host'])
    spark.conf.set("spark.sql.catalog.clickhouse.protocol", config.get('clickhouse_protocol', 'http'))
    spark.conf.set("spark.sql.catalog.clickhouse.http_port", config['clickhouse_port'])
    spark.conf.set("spark.sql.catalog.clickhouse.user", config['clickhouse_user'])
    spark.conf.set("spark.sql.catalog.clickhouse.password", config['clickhouse_password'])
    spark.conf.set("spark.sql.catalog.clickhouse.database", config['clickhouse_database'])
    spark.conf.set("spark.clickhouse.write.format", config.get('clickhouse_write_format', 'json'))




def init_clickhouse_client(config: Dict[str, Any]) -> Client:
    """
    初始化 ClickHouse Client。
    """
    return Client(
        host=config['clickhouse_host'],
        port=int(config['clickhouse_tcp_port']),
        user=config['clickhouse_user'],
        password=config['clickhouse_password'],
        database=config['clickhouse_database']
    )