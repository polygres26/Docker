SET ECHO ON
SET PAGESIZE 100
SET LINESIZE 200
SET FEEDBACK ON
PROMPT ===TEST=== dual
SELECT 1 FROM DUAL;
PROMPT ===TEST=== sysdate
SELECT SYSDATE FROM DUAL;
PROMPT ===TEST=== v$version
SELECT * FROM V$VERSION;
PROMPT ===TEST=== v$instance
SELECT INSTANCE_NUMBER, STATUS FROM V$INSTANCE;
PROMPT ===TEST=== v$session
SELECT COUNT(*) FROM V$SESSION;
PROMPT ===TEST=== nonexistent object reference
SELECT * FROM DEFINITELY_NOT_A_REAL_TABLE;
PROMPT ===TEST=== dba_tables
SELECT COUNT(*) FROM DBA_TABLES;
PROMPT ===TEST=== create and use a real table
CREATE TABLE emp_test (id NUMBER, name VARCHAR2(50));
INSERT INTO emp_test VALUES (1, 'Alice');
INSERT INTO emp_test VALUES (2, 'Bob');
SELECT * FROM emp_test;
PROMPT ===TEST=== user_tables (Oracle-native object reference)
SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME = 'EMP_TEST';
PROMPT ===TEST=== dbms_output
SET SERVEROUTPUT ON
BEGIN
  DBMS_OUTPUT.PUT_LINE('hello from sqlcl through orawire');
END;
/
PROMPT ===TEST=== sys_context
SELECT SYS_CONTEXT('USERENV','CURRENT_USER') FROM DUAL;
SELECT SYS_CONTEXT('USERENV','SESSIONID') FROM DUAL;
PROMPT ===TEST=== to_char with NLS default format
SELECT TO_CHAR(SYSDATE) FROM DUAL;
PROMPT ===TEST=== to_char with explicit RR format
SELECT TO_CHAR(SYSDATE, 'DD-MON-RR') FROM DUAL;
PROMPT ===TEST=== dbms_random
SELECT DBMS_RANDOM.STRING('U', 8) FROM DUAL;
PROMPT ===TEST=== dbms_crypto hash
SELECT DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW('hello'), DBMS_CRYPTO.HASH_SH256) FROM DUAL;
PROMPT ===TEST=== a genuinely bad PL/SQL reference
BEGIN
  this_procedure_does_not_exist();
END;
/
PROMPT ===TEST=== a genuinely bad SQL syntax error
SELECT * FROM emp_test WHERE;
PROMPT ===TEST=== division by zero
SELECT 1/0 FROM DUAL;
PROMPT ===TEST=== rowid/rownum pseudo columns
SELECT ROWNUM, name FROM emp_test WHERE ROWNUM <= 1;
PROMPT ===TEST=== dbms_metadata (not implemented -- expect error)
SELECT DBMS_METADATA.GET_DDL('TABLE','EMP_TEST') FROM DUAL;
PROMPT ===TEST=== cleanup
DROP TABLE emp_test;
PROMPT ===TEST=== done
EXIT;
