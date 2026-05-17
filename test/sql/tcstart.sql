-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO sample;

-- list imported tables
\detr+ sample.*
