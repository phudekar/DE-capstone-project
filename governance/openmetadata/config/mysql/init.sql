-- OpenMetadata MySQL initialisation
-- Creates the two databases required by the OpenMetadata stack:
--   openmetadata_db  – used by the OpenMetadata server
--   airflow_db       – used by the Airflow-based ingestion service

CREATE DATABASE IF NOT EXISTS openmetadata_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS airflow_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Grant all privileges on both databases to the application user
GRANT ALL PRIVILEGES ON openmetadata_db.* TO 'openmetadata_user'@'%';
GRANT ALL PRIVILEGES ON airflow_db.*       TO 'openmetadata_user'@'%';

FLUSH PRIVILEGES;
