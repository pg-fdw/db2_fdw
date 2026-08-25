--
-- TC029: import DB2 DECFLOAT columns without losing decimal precision
--
-- SQL_DECFLOAT is -360 in DB2 CLI. PostgreSQL float(8) is real/float4,
-- which keeps only about six decimal digits and can turn 130023450 into
-- 130023000 when converted to numeric. DECFLOAT has variable scale, so the
-- lossless local type is unconstrained numeric rather than numeric(p, s).
--
SELECT count(*) > 0 AS found_decfloat_columns,
       coalesce(bool_and(a.atttypid = 'numeric'::regtype
                         AND a.atttypmod = -1), false)
         AS all_decfloat_columns_are_unconstrained_numeric
FROM pg_attribute a
JOIN pg_class c
  ON c.oid = a.attrelid
JOIN pg_namespace n
  ON n.oid = c.relnamespace
CROSS JOIN LATERAL pg_options_to_table(a.attfdwoptions) opt
WHERE n.nspname = 'sample'
  AND c.relkind = 'f'
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND opt.option_name = 'db2type'
  AND opt.option_value = '-360';

--
-- END of TC029
--
