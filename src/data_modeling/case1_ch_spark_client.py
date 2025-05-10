# 基本模組
import os
import time
import json
import logging
from typing import Dict, Any

# 外部套件
from dotenv import load_dotenv
from clickhouse_driver import Client
from pyspark.sql import SparkSession
from src.utils   import load_config, init_spark, load_from_mongodb, configure_clickhouse_spark, CLICKHOUSE_TABLE_NAME,init_clickhouse_client

# 日誌設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



class ClickHouseClientWrapper: # 保留原有的封裝類別，但內部使用 utils 的初始化函數
    def __init__(self, config):
        self.client = init_clickhouse_client(config)

    def execute(self, query, description="SQL 查詢"):
        try:
            result = self.client.execute(query)
            logging.info(f"{description} 成功執行。")
            return result
        except Exception as e:
            logging.error(f"{description} 失敗: {e}")
            return None




def create_source_table(spark: SparkSession, config: Dict[str, Any], table_name: str):
    """
    Create ClickHouse table
    Args:
        spark (SparkSession): Spark session
        config (Dict[str, Any]): Configuration dictionary
        table_name (str): Table name to create
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS clickhouse.{table_name}
    (
        `_id` String NOT NULL,
        `timestamp` TIMESTAMP  NOT NULL,
        `temperature` Double,
        `pressure` Double,
        `vibration` Double,
        `energy_consumption` Double,
        `humidity` Double,
        `anomaly_flag` Boolean,
        `downtime_risk` Double,
        `failure_code` String,
        `remaining_life` Int,
        `machine_id` Int,
        `machine_status` String,
        `maintenance_required` Boolean,
        `month_partition` Date NOT NULL
    )
    USING clickhouse
    PARTITIONED BY (month_partition)
    TBLPROPERTIES (
        engine = 'MergeTree()',
        order_by = 'timestamp',
        settings.index_granularity = 8192
    );
    """
    print(create_table_sql)
    spark.sql(create_table_sql)




# ✅ 使用 MVIEW + 實體表
def create_tables_with_spark(spark: SparkSession, config: Dict[str, Any]):
    tables = {
        "dm2a_anomaly_summary": """
            CREATE TABLE IF NOT EXISTS clickhouse.De_Training.sp_anomaly_summary (
                machine_id String NOT NULL, 
                anomaly_count Int,
                month Date NOT NULL
            )
            TBLPROPERTIES (
                engine = 'MergeTree()',
                order_by = 'month,machine_id',
                settings.index_granularity = 8192
            );
        """,
        "dm_energy_efficiency": """
            CREATE TABLE IF NOT EXISTS clickhouse.De_Training.sp_energy_efficiency (
                machine_id String NOT NULL,
                max_energy Double,
                avg_energy Double,
                avg_machine_status Double
            )
            TBLPROPERTIES (
                engine = 'MergeTree()',
                order_by = 'machine_id',
                settings.index_granularity = 8192
            );
        """,
        "dm_monthly_temp": """
            CREATE TABLE IF NOT EXISTS clickhouse.De_Training.sp_monthly_temp (
                month Date NOT NULL, 
                machine_id String NOT NULL,
                avg_temperature Double
            )
            PARTITIONED BY (month)
            TBLPROPERTIES (
                engine = 'MergeTree()',
                order_by = 'month,machine_id',
                settings.index_granularity = 8192
            );
        """
    }

    for name, ddl in tables.items():
        
        try:
            spark.sql(ddl)
            logging.info(f"Spark 成功創建表: {name}")
        except Exception as e:
            logging.error(f"Spark 創建表 {name} 失敗: {e}")

def create_mviews_with_clickhouse_client(clickhouse_client_wrapper: ClickHouseClientWrapper,database:str,table_name:str): # 使用封裝類別
    views = {
        "anomaly_summary_mv": f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.sp_ch_anomaly_summary_mv
            TO {database}.sp_anomaly_summary AS
            SELECT
                machine_id,
                countIf(anomaly_flag = 1) AS anomaly_count,
                toStartOfMonth(timestamp) AS month
            FROM {table_name}
            GROUP BY machine_id, month
        """,
        "energy_efficiency_mv": f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS  {database}.sp_ch_energy_efficiency_mv
            TO {database}.sp_energy_efficiency AS
            SELECT
                machine_id,
                max(CAST(assumeNotNull(energy_consumption) AS Float64)) AS max_energy,
                avg(CAST(energy_consumption AS Float64)) AS avg_energy,
                avg(CAST(machine_status AS Float64)) AS avg_machine_status
            FROM {table_name}
            GROUP BY machine_id
        """,
        "monthly_temp_mv": f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS  {database}.sp_ch_monthly_temp_mv
            TO {database}.sp_monthly_temp AS
            SELECT
                machine_id,
                toStartOfMonth(toDate(timestamp)) AS month,
                avg(CAST(temperature AS Float64)) AS avg_temperature
            FROM {table_name}
            WHERE timestamp >= now() - INTERVAL 3 MONTH
            GROUP BY machine_id, month
        """
    }

    for name, query in views.items():
        clickhouse_client_wrapper.execute(query, f"創建物化視圖 {name}")




def main():
    
    start_time = time.time()
    config = load_config()
    # clickhouse_client = init_clickhouse_client(config)

    clickhouse_client_wrapper = ClickHouseClientWrapper(config) # 使用封裝類別
    database = config['clickhouse_database']
    table_name= f"{database}.{CLICKHOUSE_TABLE_NAME}"
    spark = None
    try:
        spark = init_spark("case1_spark_ddl_pipeline",config)
        configure_clickhouse_spark(spark, config)
        # ✅ 使用 Spark 建立表

        create_source_table(spark, config,table_name)
        create_tables_with_spark(spark, config)
        # ✅ 使用 ClickHouse Client 建立物化視圖
        create_mviews_with_clickhouse_client(clickhouse_client_wrapper,database,table_name)


        time.sleep(5)

    except Exception as e:
        print(f"[ERROR] {str(e)}")
    finally:
        if spark:
            spark.stop()
        print(f"[INFO] Job finished in {time.time() - start_time:.2f} seconds")

