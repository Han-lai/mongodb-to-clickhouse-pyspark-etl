import kagglehub
import os
import shutil
from pathlib import Path
# DATA_DIR = "../data/raw"

def main():
    # 下載 Kaggle 資料
    path = kagglehub.dataset_download("ziya07/smart-manufacturing-iot-cloud-monitoring-dataset")

    print(f"Dataset downloaded to {path}")

    # Step 2: 設定來源與目標路徑
    src_dir = Path(path)  # KaggleHub 預設下載的資料夾
    dst_dir = Path(__file__).resolve().parent.parent / "data" / "raw"  # 目標資料夾

    # 建立目標資料夾（如果尚未存在）
    dst_dir.mkdir(parents=True, exist_ok=True)

    # Step 3: 搬移檔案
    for filename in os.listdir(src_dir):
        src_file = src_dir / filename
        dst_file = dst_dir / filename

        if src_file.is_file():
            if dst_file.exists():
                dst_file.unlink()  # 刪除已存在的同名檔案
            shutil.copy(src_file, dst_file)
            print(f"Copied: {src_file.name} → {dst_file}")
            
          

    print(f"\nAll files moved to: {dst_dir}")
    print(f"\nAll files name: {dst_file}")
    return dst_file  # 回傳最後的檔案路徑

# if __name__ == "__main__":
#     download_and_move_kaggle_data()
    