import os
import glob
from google.cloud import bigquery

def run_silver_transformations():
    # Khởi tạo BigQuery Client
    client = bigquery.Client()
    
    # Lấy đường dẫn thư mục chứa file SQL (src/silver/sql)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(current_dir, "sql")
    
    # Lấy danh sách các file SQL và tự động sắp xếp theo tiền tố (01 -> 07)
    sql_files = sorted(glob.glob(os.path.join(sql_dir, "*.sql")))
    
    if not sql_files:
        print(f"Không tìm thấy file SQL nào trong thư mục {sql_dir}")
        return

    print(f"Bắt đầu chạy {len(sql_files)} scripts để transform dữ liệu lên tầng Silver...\n" + "-"*50)
    
    for file_path in sql_files:
        file_name = os.path.basename(file_path)
        print(f"Đang thực thi: {file_name} ...", end=" ", flush=True)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_query = f.read()
            
            # Mẹo: Sau này nếu bạn đổi project_id từ dạng hardcode sang parameter, 
            # bạn có thể truyền vào thông qua hàm .format() ở đây.
            # Ví dụ: sql_query = sql_query.format(project_id="fpt-fresher-495407")
            
            # Chạy query trên BigQuery
            query_job = client.query(sql_query)
            query_job.result()  # Wait cho job chạy xong
            
            print("THÀNH CÔNG")
            
        except Exception as e:
            print("\nTHẤT BẠI!")
            print(f"Lỗi chi tiết: {e}")
            # Tuỳ theo chiến lược orchestration, bạn có thể raise lỗi để Airflow/Pipeline bắt được
            raise e

    print("-" * 50)
    print("HOÀN THÀNH PIPELINE BRONZE -> SILVER!")

if __name__ == "__main__":
    run_silver_transformations()
