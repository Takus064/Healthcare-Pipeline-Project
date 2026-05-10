CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.ha_departments` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_a/departments/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.ha_providers` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_a/providers/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.ha_patients` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_a/patients/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.ha_transactions` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_a/transactions/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.ha_encounters` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_a/encounters/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.hb_departments` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_b/departments/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.hb_providers` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_b/providers/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.hb_patients` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_b/patients/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.hb_transactions` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_b/transactions/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `fpt-fresher-495407.bronze_dataset.hb_encounters` OPTIONS (
    format = 'JSON',
    uris = ['gs://healthcare_bucket_longnn/landing/hospital_b/encounters/*.json']
);