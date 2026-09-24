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

-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample2;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO sample2 OPTIONS (importtype 'T');
-- list imported tables
\detr+ sample2.*
DROP SCHEMA sample2 CASCADE;

-- Prepare a local schema
CREATE SCHEMA IF NOT EXISTS sample2;
-- Import the complete sample db into the local schema
IMPORT FOREIGN SCHEMA "DB2INST1" FROM SERVER sample INTO sample2 OPTIONS (importtype 'V');
-- list imported tables
\detr+ sample2.*
DROP SCHEMA sample2 CASCADE;