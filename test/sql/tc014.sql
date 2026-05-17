--
-- TC014: INSERT
--
\d+ sample.remotetable
SELECT * FROM sample.remotetable;
INSERT INTO sample.remotetable (col1, col2, col3, col4, col7, col8, col9, col10, col11, col12, col13, col24, col25) 
VALUES  ( 581927,
          'sometext',
          'sometext',
          'sometext',
          'sometext',
          current_date,
          current_time,
          current_date,
          current_time,
          999,
          'AA',
          current_timestamp,
          'sometext'
          );
SELECT * FROM sample.remotetable;
DELETE FROM sample.remotetable where col1 = 581927;
--
-- END of TC014
--
