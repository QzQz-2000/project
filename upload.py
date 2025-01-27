import os
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from concurrent.futures import ThreadPoolExecutor
import math

# Elasticsearch服务器的地址和端口
ES_HOST = "http://localhost:9200"  # 根据实际情况修改
INDEX_NAME = "2024-10-10"  # 你要上传到的 Elasticsearch 索引

# 连接到 Elasticsearch
es = Elasticsearch([ES_HOST])

# 批量大小
BATCH_SIZE = 1000  # 每批次的文档数量

# 获取所有 CSV 文件的路径
def get_csv_files(root_dir):
    csv_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

# 创建一个生成器来批量上传数据
def generate_actions(dataframe, index):
    for _, row in dataframe.iterrows():
        # 将每一行转换成 Elasticsearch 文档
        yield {
            "_op_type": "index",  # 可以是 index, create, update, delete
            "_index": index,
            "_source": row.to_dict()  # 将 DataFrame 行转换为字典
        }

# 单独上传批次的函数
def upload_batch(batch_df):
    try:
        actions = generate_actions(batch_df, INDEX_NAME)
        helpers.bulk(es, actions)
        print(f"批次上传成功, 文档数: {len(batch_df)}")
    except Exception as e:
        print(f"上传批次时出错: {e}")

# 将数据分成多个批次
def chunk_dataframe(dataframe, batch_size):
    num_chunks = math.ceil(len(dataframe) / batch_size)
    return [dataframe.iloc[i * batch_size:(i + 1) * batch_size] for i in range(num_chunks)]

# 并发读取和上传文件
def upload_data_concurrently(csv_files):
    # 使用线程池来并发处理文件
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 读取并处理每个 CSV 文件
        futures = []
        for csv_file in csv_files:
            futures.append(executor.submit(process_file, csv_file))
        
        # 等待所有任务完成
        for future in futures:
            future.result()

# 处理每个 CSV 文件
def process_file(csv_file):
    try:
        print(f"正在处理文件: {csv_file}")
        df = pd.read_csv(csv_file)
        batches = chunk_dataframe(df, BATCH_SIZE)
        
        # 使用线程池上传每个批次
        with ThreadPoolExecutor(max_workers=3) as batch_executor:
            batch_executor.map(upload_batch, batches)
        
        print(f"文件 {csv_file} 上传完成")
    except Exception as e:
        print(f"处理文件 {csv_file} 时出错: {e}")

# 获取所有 CSV 文件路径
root_dir = "2024-10-10"  # 替换为你自己的文件目录路径
csv_files = get_csv_files(root_dir)

# 开始并发处理和上传数据
upload_data_concurrently(csv_files)
