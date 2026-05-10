#!/bin/bash
# Script để kiểm tra chất lượng DAG
echo "Bắt đầu kiểm tra cú pháp DAG..."
python3 -m pytest tests/test_dag_import.py
if [ $? -eq 0 ]; then
    echo "KIỂM TRA THÀNH CÔNG!"
else
    echo "KIỂM TRA THẤT BẠI. Hủy bỏ quá trình Deploy."
    exit 1
fi
