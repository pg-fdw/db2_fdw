SELECT 'CREATE DATABASE regtest'
WHERE NOT EXISTS (
    SELECT FROM pg_database
    WHERE datname = 'regtest'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE regtest to postgres;
\c regtest
