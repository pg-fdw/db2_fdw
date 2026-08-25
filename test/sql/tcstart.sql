-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO sample;
-- list imported tables
\detr+ sample.*

-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample1;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample1 INTO sample1;
-- list imported tables
\detr+ sample1.*
