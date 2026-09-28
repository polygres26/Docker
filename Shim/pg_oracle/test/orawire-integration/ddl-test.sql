PROMPT ===create table with oracle types===
CREATE TABLE emp_test (
  id NUMBER,
  amount NUMBER(10,2),
  name VARCHAR2(50),
  notes CLOB,
  photo BLOB,
  token RAW(16)
);
PROMPT ===insert===
INSERT INTO emp_test (id, amount, name, notes) VALUES (1, 1234.56, 'Alice', 'some notes here');
PROMPT ===select===
SELECT id, amount, name, notes FROM emp_test;
PROMPT ===describe via user_tab_columns===
SELECT column_name, data_type FROM user_tab_columns WHERE table_name = 'EMP_TEST' ORDER BY column_id;
PROMPT ===cleanup===
DROP TABLE emp_test;
PROMPT ===done===
EXIT;
