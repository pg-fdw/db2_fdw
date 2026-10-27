--
-- TC034: BLOB round-trip correctness
--
-- db2GetLob.c passed the same buffer length (LOB_CHUNK_SIZE + 1) to
-- SQLGetData for CLOBs and BLOBs. With SQL_C_CHAR the extra byte holds
-- the 0 terminator, but SQL_C_BINARY has none and the driver filled all
-- LOB_CHUNK_SIZE + 1 bytes, of which only LOB_CHUNK_SIZE were copied:
-- every full 8 KiB chunk of a BLOB silently lost its last byte.
--
-- Uses the bitmaps in the SAMPLE database's DB2INST1.EMP_PHOTO (BLOB(100K),
-- 43-74 KB each). A BMP file stores its own size in header bytes 2-5
-- (little endian), so the fetched length can be checked against it.
--
SELECT empno, length(picture) AS fetched_len,
       get_byte(picture,2) + get_byte(picture,3)*256 + get_byte(picture,4)*65536 + get_byte(picture,5)*16777216 AS bmp_size,
       length(picture) = get_byte(picture,2) + get_byte(picture,3)*256 + get_byte(picture,4)*65536 + get_byte(picture,5)*16777216 AS length_matches
FROM sample.emp_photo WHERE photo_format = 'bitmap' ORDER BY empno;
--
-- END of TC034
--
