
from typing import Dict, Any
from pyspark.sql.functions import col, to_timestamp, regexp_extract, regexp_replace,trunc, lit, when, col
from src.utils   import load_config, init_spark, load_from_mongodb, configure_clickhouse_spark, CLICKHOUSE_TABLE_NAME
import time


def transform_data(df):
    """Cleans and transforms the input DataFrame."""
    return df.withColumn("timestamp", to_timestamp(regexp_replace(col("timestamp"), "Date: ", ""), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("machine_id", when(
            regexp_extract(col("machine_id"), r"Machine_(\d+)", 1) != "",
            regexp_extract(col("machine_id"), r"Machine_(\d+)", 1).cast("int")
        ).otherwise(lit(None))) \
        .withColumn("humidity", when(
            regexp_replace(col("readings.humidity"), "humid_value:", "") != "",
            regexp_replace(col("readings.humidity"), "humid_value:", "").cast("double")
        ).otherwise(lit(None))) \
        .withColumnRenamed("life_remaining", "remaining_life") \
        .withColumn("energy_consumption", col("readings.energy_use")) \
        .withColumn("month_partition", trunc(col("timestamp"), "month").cast("date")) \
        .select(
            col("_id").cast("string"),
            col("anomaly_flag").cast("boolean"),
            col("downtime_risk").cast("double"),
            col("failure_code").cast("string"),
            col("remaining_life").cast("int"),
            col("machine_id").cast("int"),
            col("machine_status").cast("string"),
            col("maintenance_required").cast("boolean"),
            col("timestamp"),
            col("readings.temperature").alias("temperature").cast("double"),
            col("readings.pressure").alias("pressure").cast("double"),
            col("readings.vibration").alias("vibration").cast("double"),
            col("energy_consumption").cast("double"),
            col("humidity"),
            col("month_partition")
        )


def write_to_clickhouse(df, config: Dict[str, Any], table_name: str, mode: str = "append"):
    """
    Write DataFrame to ClickHouse
    
    Args:
        df: PySpark DataFrame
        config (Dict[str, Any]): Configuration dictionary
        table_name (str): Target table name
        mode (str, optional): Write mode. Defaults to "append".
    """
    full_table_name = f"clickhouse.{config['clickhouse_database']}.{table_name}"
    df.write.option("batchsize", 10000).mode(mode).saveAsTable(full_table_name)


def main():
    start_time = time.time()
    config = load_config()
    print(f"[DEBUG] MongoDB URI from config: {config['mongodb_uri']}") # 添加這行

    spark = None
    try:
        spark = init_spark("case1_native_pipeline",config)
        configure_clickhouse_spark(spark, config)
        mongo_df = load_from_mongodb(spark, config)
        cleaned_df = transform_data(mongo_df)
        write_to_clickhouse(cleaned_df, config, CLICKHOUSE_TABLE_NAME)

    except Exception as e:
        print(f"[ERROR] {str(e)}")
    finally:
        if spark:
            spark.stop()
        print(f"[INFO] Job finished in {time.time() - start_time:.2f} seconds")
