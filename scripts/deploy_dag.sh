#!/bin/bash
# Script để đồng bộ code lên Cloud Storage
echo "Bắt đầu đồng bộ code lên GCS..."

# 1. Đồng bộ DAGs và SQL
gsutil -m rsync -d -r dags/ gs://${_COMPOSER_BUCKET}/dags/

# 2. Đồng bộ Source Code PySpark
gsutil -m rsync -d -r src/ gs://${_COMPOSER_BUCKET}/scripts/

echo "ĐỒNG BỘ THÀNH CÔNG!"
