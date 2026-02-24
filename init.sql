-- Initialize Data Warehouse Database for Perusahaan XYZ
-- Created: 2026-02-15

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS sales_analytics;
CREATE SCHEMA IF NOT EXISTS nlp_data;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE xyz_warehouse TO dw_user;
GRANT ALL PRIVILEGES ON SCHEMA sales_analytics TO dw_user;
GRANT ALL PRIVILEGES ON SCHEMA nlp_data TO dw_user;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'Data Warehouse initialized successfully!';
    RAISE NOTICE 'Database: xyz_warehouse';
    RAISE NOTICE 'User: dw_user';
END $$;