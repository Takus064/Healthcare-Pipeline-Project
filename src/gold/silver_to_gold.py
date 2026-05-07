import os
import glob
from google.cloud import bigquery

def run_gold_transformations():
    # Khởi tạo BigQuery Client
    client = bigquery.Client()
    
    # Lấy đường dẫn thư mục chứa file SQL tầng Gold
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(current_dir, "sql")
    
    # Ở đây các file SQL nằm trong thư mục con 'sql'
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))
    
    if not sql_files:
        print(f"Không tìm thấy file SQL nào trong thư mục {sql_dir}")
        return

    print(f"Bắt đầu chạy {len(sql_files)} scripts để xây dựng báo cáo tầng Gold...\n" + "-"*50)
    
    for file_path in sql_files:
        file_name = os.path.basename(file_path)
        print(f"Đang thực thi: {file_name} ...", end=" ", flush=True)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_query = f.read()
            
            # Chạy query tạo bảng Gold
            query_job = client.query(sql_query)
            query_job.result()  # Đợi thực thi hoàn tất
            
            print("THÀNH CÔNG")
            
        except Exception as e:
            print("\nTHẤT BẠI!")
            print(f"Lỗi chi tiết: {e}")
            raise e

    print("-" * 50)
    print("HOÀN THÀNH PIPELINE SILVER -> GOLD!")

if __name__ == "__main__":
    run_gold_transformations()
