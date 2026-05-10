# Healthcare Data Pipeline - CI/CD Project

Dự án này xây dựng một hệ thống Data Pipeline tự động hóa cho ngành Y tế (Healthcare) sử dụng bộ công cụ trên Google Cloud Platform (GCP). Hệ thống thực hiện việc nạp dữ liệu (Ingestion), biến đổi (Transformation) qua các tầng Bronze - Silver - Gold và được quản lý bởi quy trình CI/CD nghiêm ngặt.

## 🏗 Kiến trúc hệ thống
Dự án tích hợp các dịch vụ:
- **Google Cloud Composer (Airflow)**: Điều phối toàn bộ luồng công việc (Orchestration).
- **Google Cloud Dataproc (Spark)**: Xử lý dữ liệu lớn ở tầng Ingestion và Bronze.
- **Google BigQuery**: Kho lưu trữ dữ liệu (Data Warehouse) cho tầng Silver và Gold.
- **Google Cloud Build**: Tự động hóa quy trình CI/CD.
- **Google Cloud Storage (GCS)**: Lưu trữ file Landing và code scripts.

## 📂 Cấu trúc thư mục dự án
Cấu trúc được thiết kế chuẩn theo yêu cầu của Assignment 02:

```bash
Healthcare-Project/
├── dags/                       # Chứa file định nghĩa Airflow DAG
│   ├── healthcare_pipeline_dag.py
│   └── sql/                    # Chứa các câu lệnh SQL cho tầng Silver/Gold
│       ├── silver/
│       └── gold/
├── scripts/                    # Chứa các script bổ trợ CI/CD
│   ├── validate_dag.sh         # Script kiểm tra cú pháp DAG
│   ├── deploy_dag.sh           # Script đồng bộ code lên GCS
│   └── trigger_dag.sh          # Script kích hoạt DAG tự động
├── src/                        # Chứa source code xử lý logic (PySpark)
│   ├── ingestion/
│   ├── bronze/
│   └── silver/
├── tests/                      # Chứa các unit test cho CI/CD
│   └── test_dag_import.py
├── cloudbuild.yaml             # File cấu hình luồng CI/CD
├── requirements.txt            # Khai báo thư viện phụ thuộc
├── configs/                    # File cấu hình metadata (CSV, JSON)
└── README.md
```

## 🔄 Quy trình CI/CD
Quy trình được thực hiện tự động bởi Cloud Build mỗi khi có hành động `Push` code lên Github:

1.  **Checkout & Install**: Lấy mã nguồn mới nhất và cài đặt các thư viện cần thiết.
2.  **Validate**: Chạy `test_dag_import.py` bằng `pytest`. **Quan trọng**: Nếu code DAG có lỗi, quá trình sẽ dừng lại ngay lập tức để bảo vệ môi trường Production.
3.  **Deploy**: Sử dụng `gsutil rsync` để đồng bộ hóa code lên GCS của Composer và Dataproc.
4.  **Trigger**: Sử dụng lệnh `gcloud composer` để yêu cầu Airflow thực thi DAG ngay lập tức với mã nguồn vừa cập nhật.

## 🛠 Hướng dẫn thiết lập CI/CD Trigger
Để hệ thống hoạt động, cần cấu hình các **Substitution variables** sau trên Cloud Build Trigger:
- `_COMPOSER_BUCKET`: Đường dẫn Bucket của Composer (dags folder).
- `_COMPOSER_ENV_NAME`: Tên môi trường Cloud Composer.
- `_LOCATION`: Vùng (ví dụ: `asia-southeast1`).

## 📊 Kết quả đạt được
- Toàn bộ dữ liệu từ Hospital A/B được hợp nhất và làm sạch.
- Dữ liệu nhạy cảm (PII) được mã hóa bằng SHA256 tại tầng Silver.
- Các báo cáo tài chính (Gold layer) được tính toán chính xác, không bị trùng lặp dữ liệu.
