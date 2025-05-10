
import time
from typing import Dict, Any
from src.utils  import load_config, init_spark, load_from_mongodb, CLICKHOUSE_TABLE_NAME
from pyspark.sql.functions import col, to_timestamp, regexp_extract, regexp_replace, trunc


def transform_data(df):
    """Cleans and transforms the DataFrame."""
    return df.withColumn("timestamp", to_timestamp(regexp_replace(col("timestamp"), "Date: ", ""), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("machine_id", regexp_extract(col("machine_id"), r"Machine_(\d+)", 1).cast("int")) \
        .withColumn("humidity", regexp_replace(col("readings.humidity"), "humid_value:", "").cast("float")) \
        .withColumnRenamed("life_remaining", "remaining_life") \
        .withColumn("energy_consumption", col("readings.energy_use")) \
        .withColumn("month_partition", trunc(col("timestamp"), "month").cast("date")) \
        .select(
            col("_id").cast("string"),
            col("anomaly_flag").cast("boolean"),
            col("downtime_risk").cast("float"),
            col("failure_code").cast("string"),
            col("remaining_life").cast("int"),
            col("machine_id").cast("int"),
            col("machine_status").cast("string"),
            col("maintenance_required").cast("boolean"),
            col("timestamp"),
            col("readings.temperature").alias("temperature").cast("float"),
            col("readings.pressure").alias("pressure").cast("float"),
            col("readings.vibration").alias("vibration").cast("float"),
            col("energy_consumption").cast("float"),
            col("humidity"),
            col("month_partition")
        )

def write_to_clickhouse(df, config: Dict[str, Any], table_name: str, mode: str = "append"):
    """Writes DataFrame to ClickHouse using JDBC."""
    jdbc_url = f"jdbc:clickhouse://{config['clickhouse_host']}:{config['clickhouse_port']}/{config['clickhouse_database']}"
    table_fqn = f"{config['clickhouse_database']}.{table_name}_jdbc"
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_fqn) \
        .option("user", config['clickhouse_user']) \
        .option("password", config['clickhouse_password']) \
        .mode(mode) \
        .save()

def main():
    start_time = time.time()
    config = load_config()
    print(f"[DEBUG] MongoDB URI from config: {config['mongodb_uri']}") # 添加這行

    spark = None
    try:
        spark = init_spark("case1_jdbc_pipeline",config)
        mongo_df = load_from_mongodb(spark, config)
        transformed_df = transform_data(mongo_df)
        # create_clickhouse_table(config, CLICKHOUSE_TABLE_NAME)
        write_to_clickhouse(transformed_df, config,CLICKHOUSE_TABLE_NAME)
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        if spark:
            spark.stop()
        print(f"[INFO] Job finished in {time.time() - start_time:.2f} seconds")

