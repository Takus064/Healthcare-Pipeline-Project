#!/bin/bash
# Script để kích hoạt DAG chạy ngay lập tức sau khi deploy
echo "Đang kích hoạt DAG: ${_DAG_ID} trên Composer..."

gcloud composer environments run ${_COMPOSER_ENV_NAME} \
    --location ${_LOCATION} \
    dags trigger -- ${_DAG_ID}

if [ $? -eq 0 ]; then
    echo "KÍCH HOẠT THÀNH CÔNG!"
else
    echo "KÍCH HOẠT THẤT BẠI. Vui lòng kiểm tra lại cấu hình Composer."
    exit 1
fi
