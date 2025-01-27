import os
import pandas as pd
from elasticsearch import Elasticsearch
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import re

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Elasticsearch 连接设置
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# 设置线程池大小
MAX_WORKERS = 5

# 遍历目录并上传 CSV 数据到 Elasticsearch
def upload_to_elasticsearch(data, index_name):
    try:
        # 将数据上传到指定的 Elasticsearch 索引
        response = es.index(index=index_name, body=data)
        logger.info(f"Document uploaded to {index_name}: {response['_id']}")
    except Exception as e:
        logger.error(f"Failed to upload document to {index_name}: {e}")

# 处理单个 CSV 文件
def process_csv_file(csv_file, platform, date):
    try:
        # 读取 CSV 文件
        df = pd.read_csv(csv_file)

        # 构建索引名称
        index_name = f"{platform}-{date}-full"

        # 使用线程池并发上传数据
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []

            # 遍历 DataFrame 的每一行，上传为文档
            for index, row in df.iterrows():
                data = row.to_dict()
                futures.append(executor.submit(upload_to_elasticsearch, data, index_name))

            # 等待所有任务完成
            for future in as_completed(futures):
                future.result()

        logger.info(f"All documents from {csv_file} uploaded successfully.")
    except Exception as e:
        logger.error(f"Error processing CSV file {csv_file}: {e}")

# 遍历目录，找到所有 CSV 文件并上传
def process_directory(base_dir):
    try:
        # 遍历目录下的子目录（平台）
        for platform_dir in os.listdir(base_dir):
            platform_path = os.path.join(base_dir, platform_dir)

            # 确保是目录
            if os.path.isdir(platform_path):
                # 提取平台名称和日期
                match = re.match(r"([a-zA-Z]+)-(\d{4}-\d{2}-\d{2})", platform_dir)
                if match:
                    platform = match.group(1)
                    date = match.group(2)

                    # 遍历当前平台目录中的所有 CSV 文件
                    for file_name in os.listdir(platform_path):
                        if file_name.endswith(".csv"):
                            csv_file = os.path.join(platform_path, file_name)
                            logger.info(f"Processing file: {csv_file}")

                            # 处理 CSV 文件并上传数据
                            process_csv_file(csv_file, platform, date)

    except Exception as e:
        logger.error(f"Error processing directory {base_dir}: {e}")

# 主函数，设置 base_dir 为 'extracted' 目录路径
if __name__ == "__main__":
    base_dir = "extracted"  # 替换为你的 extracted 目录路径
    process_directory(base_dir)

