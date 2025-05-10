# MongoDB-ClickHouse-Spark ETL Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive Data Engineering (DE) pipeline demonstrating ETL processes using IoT sensor data. This project showcases data extraction from Kaggle, transformation using various methods, and loading into ClickHouse for high-performance analytics with Spark.

## Overview

This end-to-end data processing solution demonstrates:
- **Extract**: Downloading IoT sensor datasets from Kaggle
- **Transform**: Data cleansing and transformation from MongoDB to ClickHouse via JDBC and Native approaches
- **Load**: Table creation, aggregation, and materialized view optimization in ClickHouse
- **Analysis**: High-performance analytics using Spark SQL

## Project Structure

```
├─ scripts/                              # Kaggle data download scripts
├── src/
│   ├── connect_config/
│   │   ├──.env                          # Environment variables storage
│   ├── data_analysis/                   
│   │   ├── case1_ch_spark_query.py      # Spark data analysis queries
│   ├── data_cleaning/
│   │   ├── case1_md_2_ch_env_jdbc.py    # Case 1: MongoDB to ClickHouse via JDBC
│   │   └── case1_md_2_ch_env_native.py  # Case 1: MongoDB to ClickHouse via Native method
│   ├── data_modeling/
│   │   └── case1_ch_agg_client.py       # Case 1: ClickHouse Client for source tables and materialized views
│   │   └── case1_ch_spark_client.py     # Case 1: Spark SQL for source tables and materialized views
│   └── utils.py                         # Shared utility functions
├── main.py                              # Pipeline entry point
└── README.md
```

## Prerequisites

- Python 3.x
- MongoDB server (running and accessible)
- ClickHouse server (running and accessible)
- Apache Spark (for 'native' case and data analysis)
- Kaggle API access (for downloading datasets)

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/mongodb-clickhouse-spark-etl-pipeline.git
   cd mongodb-clickhouse-spark-etl-pipeline
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   # venv\Scripts\activate   # On Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   Create a `.env` file in the `connect_config` directory with the following content:
   ```
   MONGODB_URI=mongodb://<username>:<password>@<host>:<port>/
   MONGODB_DB=your_mongodb_database
   MONGODB_COLLECTION=your_mongodb_collection

   CLICKHOUSE_HOST=your_clickhouse_host
   CLICKHOUSE_PORT=8123
   CLICKHOUSE_TCP_PORT=9000
   CLICKHOUSE_USER=your_clickhouse_user
   CLICKHOUSE_PASSWORD=your_clickhouse_password
   CLICKHOUSE_DATABASE=your_clickhouse_database
   CLICKHOUSE_TABLE_NAME=sensor_data
   ```

5. **Configure Kaggle API:**
   Place your `kaggle.json` credentials file in the appropriate location or set environment variables for accessing Kaggle datasets.

## Usage

### Downloading Data from Kaggle

To download the IoT sensor dataset from Kaggle:
```bash
python scripts/download_kaggle_data.py
```

### Running the ETL Pipeline

Execute the pipeline with one of the following methods:

**Native method (using Spark):**
```bash
python main.py native
```

**JDBC method:**
```bash
python main.py jdbc
```

**View usage instructions:**
```bash
python main.py
```

## Pipeline Process

1. **Data Extraction**: 
   - Downloads IoT sensor dataset from Kaggle
   - Loads data into MongoDB

2. **Data Transformation**:
   - Cleans and normalizes the data
   - Transforms data structure according to analysis requirements
   - Two implementation options:
     - JDBC connector for direct MongoDB to ClickHouse transfer
     - Native method using Spark as intermediary

3. **Data Loading**:
   - Creates optimized table structures in ClickHouse
   - Establishes materialized views for faster analytics
   - Configures proper indexes and sorting keys

4. **Data Analysis**:
   - Performs analytics queries using Spark
   - Connects to ClickHouse for high-performance data retrieval
   - Executes pre-defined analytical scenarios

## Performance Considerations

- The "native" method leverages Spark's distributed processing capabilities for handling large datasets
- The JDBC approach provides a simpler implementation with direct database connections
- ClickHouse materialized views significantly improve query performance for common analysis patterns
- Memory usage should be monitored when processing large IoT datasets

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- IoT sensor dataset from Kaggle
- The MongoDB, ClickHouse, and Apache Spark communities
- All contributors to this project

## Author

Albee.lai