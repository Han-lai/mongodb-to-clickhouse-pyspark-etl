# database_setup.py

import logging
import time
from typing import Dict, Any

from src.utils import load_config, init_clickhouse_client, CLICKHOUSE_TABLE_NAME

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


def create_clickhouse_source_table(clickhouse_client_wrapper: ClickHouseClientWrapper, config: Dict[str, Any], table_name: str):
    """Creates ClickHouse table with MergeTree engine and monthly partitioning."""

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            `_id` String NOT NULL,
            `timestamp` DateTime NOT NULL,
            `temperature` Float32,
            `pressure` Float32,
            `vibration` Float32,
            `energy_consumption` Float32,
            `humidity` Float32,
            `anomaly_flag` UInt8,  -- 使用 UInt8 代表 Boolean (ClickHouse 常用)
            `downtime_risk` Float32,
            `failure_code` String,
            `remaining_life` Int32,
            `machine_id` Int32,
            `machine_status` String,
            `maintenance_required` UInt8, -- 使用 UInt8 代表 Boolean
            `month_partition` Date NOT NULL
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp)
        PARTITION BY toYYYYMM(month_partition);
    """
    try:
        clickhouse_client_wrapper.execute(create_table_sql, f"創建 Source Table: {table_name}")
        logging.info(f"成功創建 Source Table: {table_name}")
    except Exception as e:
        logging.error(f"創建 Source Table {table_name} 失敗: {e}")
    

def create_tables_with_jdbc(clickhouse_client_wrapper: ClickHouseClientWrapper): # 使用封裝類別
    tables = {
        "monthly_anomaly_counts": """
        CREATE TABLE IF NOT EXISTS De_Training.agg_monthly_anomaly_counts
          (
            machine_id String NOT NULL,
            anomaly_count  Float64,
            month Date NOT NULL
            )
        ENGINE = MergeTree()
        PARTITION BY (month)
        ORDER BY (month, machine_id);
        """,
        "dm_energy_efficiency": """
            CREATE TABLE IF NOT EXISTS De_Training.agg_energy_efficiency (
                machine_id String NOT NULL,
                max_energy AggregateFunction(max,  Float64),
                avg_energy AggregateFunction(avg,  Float64),
                avg_machine_status AggregateFunction(avg,  Float64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY machine_id
            SETTINGS index_granularity = 8192;
        """,
        "dm_monthly_temp": """
            CREATE TABLE IF NOT EXISTS De_Training.agg_monthly_temp (
                month Date NOT NULL,
                machine_id String NOT NULL,
                avg_temp AggregateFunction(avg, Float64)
            )
            ENGINE = AggregatingMergeTree()
            ORDER BY (month, machine_id)
            SETTINGS index_granularity = 8192;
        """
    }
    for name, ddl in tables.items():
        try:
            clickhouse_client_wrapper.execute(ddl, f"創建表: {name}")
            logging.info(f"jdbc 成功創建表: {name}")
        except Exception as e:
            logging.error(f"jdbc 創建表 {name} 失敗: {e}")

def create_mviews_with_clickhouse_client(clickhouse_client_wrapper: ClickHouseClientWrapper, table_name: str): # 使用封裝類別
    views = {

        "anomaly_summary_mv": f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS De_Training.agg_anomaly_counts_mv
            TO De_Training.agg_monthly_anomaly_counts AS
            SELECT
                CAST(machine_id AS String) AS machine_id,
                countIf(anomaly_flag = 1) AS anomaly_count,
                toStartOfMonth(timestamp) AS month
            FROM {table_name}
            GROUP BY machine_id, month;
        """,
        "energy_efficiency_mv":f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS De_Training.agg_energy_efficiency_mv
            TO De_Training.agg_energy_efficiency AS
            SELECT
                CAST(machine_id AS String) AS machine_id,
                maxState(CAST(assumeNotNull(energy_consumption) AS Float64)) AS max_energy,
                avgState(CAST(energy_consumption AS Float64)) AS avg_energy,
                avgState(CAST(machine_status AS Float64)) AS avg_machine_status
            FROM {table_name}
            GROUP BY machine_id
        """,
        "monthly_temp_mv": f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS De_Training.agg_monthly_temp_mv
            TO De_Training.agg_monthly_temp AS
            SELECT
                CAST(machine_id AS String) AS machine_id,
                toStartOfMonth(toDate(timestamp)) AS month,
                avgState(CAST(temperature AS Float64)) AS avg_temp
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
    table_name= f"{database}.{CLICKHOUSE_TABLE_NAME}_jdbc"

    try:
        create_clickhouse_source_table(clickhouse_client_wrapper,config, table_name)
        create_tables_with_jdbc(clickhouse_client_wrapper)
        create_mviews_with_clickhouse_client(clickhouse_client_wrapper, table_name)
        print("[INFO] Database setup completed.")
    except Exception as e:
        print(f"[ERROR] Database setup failed: {str(e)}")
    finally:
        print(f"[INFO] Database setup job finished in {time.time() - start_time:.2f} seconds")
