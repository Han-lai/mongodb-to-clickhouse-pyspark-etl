# main.py
import sys
import logging
from src.data_cleaning import case1_md_2_ch_env_jdbc
from src.data_cleaning import case1_md_2_ch_env_native
from src.data_modeling import case1_ch_agg_client
from src.data_modeling import case1_ch_spark_client
from src.data_analysis import case1_ch_spark_query
from src.utils import load_config  # 範例：引用 utils.py 中的函數



# 配置基本日誌記錄
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline(case):
    if case == "native":
        print("[INFO] Running Case 1 native Pipeline...")
        case1_ch_spark_client.main()
        case1_md_2_ch_env_native.main()
        case1_ch_spark_query.main()  # 调用 data_analysis.py 的 main 函数
        
    elif case == "jdbc":
        print("[INFO] Running Case 2 jdbc Pipeline...")
        case1_ch_agg_client.main()
        case1_md_2_ch_env_jdbc.main()
       
    else:
        print(f"[ERROR] Invalid case: {case}. Please use 'native' or '2jdbc'.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        case_to_run = sys.argv[1]
        run_pipeline(case_to_run)
    else:
        print("[INFO] Please specify the case to run (e.g., 'python main.py native or python main.py jdbc ').")