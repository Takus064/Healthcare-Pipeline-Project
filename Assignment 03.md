# Assignment: Xây dựng pipeline NPI Ingestion với Airflow 

## 1. Bối cảnh
Dựa vào những gì đã xây dựng trong Assignment 01 và 02, hệ thống có một **operational database** chứa thông tin **provider** nội bộ.
``` sql
CREATE TABLE providers (
    providerid      varchar(50) NOT NULL,
    firstname       varchar(50) NOT NULL,
    lastname        varchar(50) NOT NULL,
    specialization  varchar(50) NOT NULL,
    deptid          varchar(50) NOT NULL,
    npi             bigint      NOT NULL,
    CONSTRAINT pk_providers PRIMARY KEY (providerid)
);
```

Mỗi provider nội bộ đã có cột **npi**, tuy nhiên dữ liệu này cần được:
- đối soát với NPI Registry API
- làm giàu bằng thông tin chuẩn từ nguồn ngoài
- theo dõi thay đổi theo thời gian
- phân loại trạng thái để phục vụ downstream systems

## 2. Mục tiêu assignment
Pipeline cần giải quyết 4 mục tiêu chính:
- **Validation:** Kiểm tra xem npi trong bảng providers có tồn tại hợp lệ trên NPI Registry API hay không.
- **Enrichment:** Thu thập thêm thông tin từ NPI Registry để bổ sung cho provider nội bộ.
- **Change detection:** So sánh dữ liệu NPI mới thu thập với snapshot trước đó để biết:
    - bản ghi nào là mới
    - bản ghi nào không đổi
    - bản ghi nào đã thay đổi
- **Downstream readiness:** Sinh ra dữ liệu đầu ra để downstream có thể:
    - cập nhật bảng curated/provider master
    - chạy data quality rules
    - báo cáo provider mismatch
    - ...

Các bạn cần xây dựng một pipeline có khả năng:
1. đọc dữ liệu provider từ operational DB
2. lấy danh sách NPI cần tra cứu
3. gọi NPI Registry API theo từng NPI
4. chuẩn hóa dữ liệu từ API
5. kiểm tra mức độ khớp với dữ liệu nội bộ
6. so sánh với snapshot cũ
7. phân loại record
8. lưu output xuống GCS

## 3. Yêu cầu chức năng
### 3.1. Nguôn dữ liệu
- Nguồn nội bộ: bảng **providers**
- Nguồn API: `https://npiregistry.cms.hhs.gov/api/`

Query truy vấn mẫu:
- version = 2.1
- state = CA
- city = Los Angeles
- limit = 20

### 3.2. Dữ liệu cần thu thập
Nếu không tìm thấy providers trên **NPI Registry** hãy đánh dấu `validation_status = "NPI_NOT_FOUND"`

Với mỗi NPI tìm được, gọi API chi tiết và trích xuất tối thiểu các trường thông tin sau:
- npi_id
- npi_first_name
- npi_last_name
- npi_position
- npi_organisation_name
- npi_last_updated
- enumeration_type
- refreshed_at

## 4. Output dữ liệu
Pipeline phải sinh tối thiểu 3 nhóm output

### 4.1. Validation output
Một dataset thể hiện kết quả đối soát giữa provider nội bộ và NPI Registry.

Ví dụ schema:
- providerid
- internal_firstname
- internal_lastname
- internal_specialization
- internal_deptid
- internal_npi
- npi_found
- npi_first_name
- npi_last_name
- npi_organisation_name
- npi_position
- npi_last_updated
- name_match
- validation_status: VALID/NAME_MISMATCH/NPI_NOT_FOUND
- refreshed_at


### 4.2. Snapshot output
Một snapshot chuẩn hóa của toàn bộ dữ liệu enrichment tại thời điểm chạy DAG.

Ví dụ path:
`gs://<bucket>/landing/provider_npi_snapshot/dt={{ ds }}/snapshot.json`

### 4.3. Change detection output
So sánh snapshot hiện tại với snapshot gần nhất trước đó và phân loại record thành:
- NEW
- UNCHANGED
- UPDATED