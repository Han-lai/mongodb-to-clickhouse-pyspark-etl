from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, struct, lit, concat, when,date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# 初始化 Spark Session
def init_spark(MONGODB_URI:str, MONGODB_DB: str,MONGODB_COLLECTION: str):
    return SparkSession.builder \
        .appName("CSV to MongoDB") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1") \
        .config("spark.mongodb.read.connection.uri", f"{MONGODB_URI}{MONGODB_DB}.{MONGODB_COLLECTION}?authSource=admin") \
        .config("spark.mongodb.write.connection.uri", f"{MONGODB_URI}{MONGODB_DB}.{MONGODB_COLLECTION}?authSource=admin") \
        .getOrCreate()


# 主要處理函數
def process_csv_to_mongodb(csv_path,MONGODB_URI:str, MONGODB_DB: str,MONGODB_COLLECTION: str):
    spark = init_spark(MONGODB_URI,MONGODB_DB,MONGODB_COLLECTION)
    
    # 定義資料結構
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("temperature", StringType(), True),
        StructField("vibration", StringType(), True),
        StructField("humidity", StringType(), True),
        StructField("pressure", StringType(), True),
        StructField("energy_consumption", StringType(), True),
        StructField("machine_status", StringType(), True),
        StructField("anomaly_flag", StringType(), True),
        StructField("predicted_remaining_life", StringType(), True),
        StructField("failure_type", StringType(), True),
        StructField("downtime_risk", StringType(), True),
        StructField("maintenance_required", StringType(), True)
    ])
    
    # 直接從路徑讀取CSV
    df = spark.read.csv(csv_path, header=True, schema=schema)
    
    # 轉換資料類型   #  to_timestamp(col("timestamp"), "yyyy/M/d HH:mm")) \
    df = df.withColumn("timestamp",to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
           .withColumn("machine_id", col("machine_id").cast(IntegerType())) \
           .withColumn("temperature", col("temperature").cast(DoubleType())) \
           .withColumn("vibration", col("vibration").cast(DoubleType())) \
           .withColumn("humidity", col("humidity").cast(DoubleType())) \
           .withColumn("pressure", col("pressure").cast(DoubleType())) \
           .withColumn("energy_consumption", col("energy_consumption").cast(DoubleType())) \
           .withColumn("machine_status", col("machine_status").cast(IntegerType())) \
           .withColumn("anomaly_flag", col("anomaly_flag").cast(IntegerType())) \
           .withColumn("predicted_remaining_life", col("predicted_remaining_life").cast(IntegerType())) \
           .withColumn("downtime_risk", col("downtime_risk").cast(IntegerType())) \
           .withColumn("maintenance_required", col("maintenance_required").cast(IntegerType()))
    
    # # 重組資料結構以符合要求的 MongoDB 結構
    # restructured_df = df.select(
    #     col("timestamp"),
    #     col("machine_id"),
    #     struct(
    #         col("temperature"),
    #         col("vibration"),
    #         col("humidity"),
    #         col("pressure"),
    #         col("energy_consumption")
    #     ).alias("readings"),
    #     struct(
    #         col("machine_status"),
    #         col("anomaly_flag"),
    #         col("predicted_remaining_life").alias("remaining_life"),
    #         col("failure_type"),
    #         col("downtime_risk"),
    #         col("maintenance_required")
    #     ).alias("status")
    # )
    
    # # 故意引入錯誤 #1：將timestamp轉為字串格式而非正確的ISODate格式
    df_with_errors = df.withColumn("timestamp",concat(lit("Date: "),col("timestamp").cast(StringType())))
    

    # 故意引入錯誤 #2：將machine_id存為字串而非整數
    df_with_errors = df_with_errors.withColumn("machine_id", concat(lit("Machine_"), col("machine_id").cast(StringType())))
    
    # 故意引入錯誤 #3：將readings結構混亂，部分數值放到字串中
    restructured_df = df_with_errors.select(
        col("timestamp"),
        col("machine_id"),
        struct(
            col("temperature"),  # 保持正確
            col("vibration"),    # 保持正確
            # 故意引入錯誤 #4：將humidity放入字串(正確應為只有數字)
            concat(lit("humid_value:"), col("humidity").cast(StringType())).alias("humidity"),
            col("pressure"),     # 保持正確
            # 故意引入錯誤 #5：使用錯誤的欄位名稱(正確應為energy_consumption)
            col("energy_consumption").alias("energy_use")
        ).alias("readings"),
        col("machine_status"),
        col("anomaly_flag"),
        # 故意引入錯誤 #7：改變欄位名稱(正確應為predicted_remaining_life)
        col("predicted_remaining_life").alias("life_remaining"),
        # 故意引入錯誤 #8：將failure_type轉為整數代碼而非使用原始的字串值
        (when(col("failure_type") == "Normal", 0)
         .when(col("failure_type") == "Critical", 1)
         .otherwise(99)).alias("failure_code"),
        col("downtime_risk"),
        col("maintenance_required")
    )

    # 顯示轉換後的結構
    print("轉換後的資料結構：")
    restructured_df.printSchema()
    restructured_df.show(truncate=False)
    
    # 儲存到 MongoDB
    restructured_df.write \
        .format("mongodb") \
        .option("database", MONGODB_DB) \
        .option("collection", MONGODB_COLLECTION) \
        .mode("append") \
        .save()


    print("資料已成功寫入 MongoDB")
    
    spark.stop()

if __name__ == "__main__":
    MONGODB_URI = "mongodb://admin:password@localhost:8017/"
    MONGODB_DB = "manufacturing_db"
    MONGODB_COLLECTION = "sensor_data"
    csv_path = "D:/04_DE_Training/de_training_pipeline/data/raw/smart_manufacturing_data.csv"
    process_csv_to_mongodb(csv_path, MONGODB_URI, MONGODB_DB, MONGODB_COLLECTION)

    # process_csv_to_mongodb(csv_path)