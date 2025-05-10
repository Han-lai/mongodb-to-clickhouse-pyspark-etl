# data_analysis.py
import os
import time
import logging
from typing import Dict, Any
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.utils   import load_config, init_spark, configure_clickhouse_spark, CLICKHOUSE_TABLE_NAME

# 日誌設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 載入環境變數

# # Load environment variables
# dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'connect_config', '.env')
# load_dotenv(dotenv_path)



def run_analysis_queries_with_spark(spark: SparkSession,table_name):
    queries = {
        "7日內機台平均溫度與震動": f"""
            SELECT
                DATE(timestamp) AS day,
                avg(temperature) AS avg_temp,
                avg(vibration) AS avg_vib
            FROM clickhouse.{table_name}
            WHERE machine_id = 39 AND timestamp >= now() - INTERVAL 45 DAY
            GROUP BY day
            ORDER BY day
        """,
        "高風險設備（剩餘壽命<50 且異常=1）": f"""
            SELECT machine_id,remaining_life,machine_status,anomaly_flag
            FROM clickhouse.{table_name}
            WHERE remaining_life < 50 AND anomaly_flag = 1
        """,
        "需要維修的設備": f"""
            SELECT machine_id,machine_status,maintenance_required
            FROM clickhouse.{table_name}
            WHERE maintenance_required = 1
        """
    }

    for name, sql in queries.items():
        try:
            df = spark.sql(sql)
            print(f"\n--- {name} ---")
            df.show(truncate=False)
        except Exception as e:
            logging.error(f"Spark 查詢 {name} 失敗: {e}")

def main():
    start_time = time.time()
    config = load_config()

    spark = None
    database = config['clickhouse_database']
    table_name= f"{database}.{CLICKHOUSE_TABLE_NAME}"
    try:
        
        spark = init_spark("case1_query_pipeline",config)
        configure_clickhouse_spark(spark, config)
        run_analysis_queries_with_spark(spark,table_name)
    except Exception as e:
        print(f"[ERROR] {str(e)}")
    finally:
        if spark:
            spark.stop()
        print(f"[INFO] Data analysis job finished in {time.time() - start_time:.2f} seconds")
