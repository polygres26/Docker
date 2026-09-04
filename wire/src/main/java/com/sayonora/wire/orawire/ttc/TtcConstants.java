package com.sayonora.wire.orawire.ttc;

public final class TtcConstants {

    public static final int MSG_TYPE_FUNCTION = 3;
    public static final int MSG_TYPE_ERROR = 4;
    public static final int MSG_TYPE_ROW_HEADER = 6;
    public static final int MSG_TYPE_ROW_DATA = 7;
    public static final int MSG_TYPE_PARAMETER = 8;
    public static final int MSG_TYPE_STATUS = 9;
    // Confirmed live via a real Oracle 23c self-loop capture of a real ojdbc CallableStatement
    // call with a scalar OUT parameter: the response carries the OUT value in a block whose own
    // leading byte is 11, distinct from every other message type this codebase already decodes
    // (3/4/6/7/8/9/15/16/17/21/29/34). Not independently confirmed against Oracle's own public TTC
    // documentation (none is available) -- named IO_VECTOR because that's the closest match to
    // what the structure actually carries (a vector of bind values), not because the name itself
    // is confirmed.
    public static final int MSG_TYPE_IO_VECTOR = 11;
    public static final int MSG_TYPE_PIGGYBACK = 17;
    public static final int MSG_TYPE_WARNING = 15;
    public static final int MSG_TYPE_DESCRIBE_INFO = 16;
    public static final int MSG_TYPE_BIT_VECTOR = 21;
    public static final int MSG_TYPE_END_OF_RESPONSE = 29;
    
    public static final int MSG_TYPE_PROTOCOL_EXTENDED = 34;

    public static final int FUNC_EXECUTE = 94;
    
    public static final int FUNC_REEXECUTE = 4;
    public static final int FUNC_REEXECUTE_AND_FETCH = 78;
    
    public static final int EXEC_OPTION_COMMIT_REEXECUTE = 0x1;
    public static final int FUNC_FETCH = 5;
    public static final int FUNC_LOGOFF = 9;
    public static final int FUNC_COMMIT = 14;
    public static final int FUNC_ROLLBACK = 15;
    public static final int FUNC_CLOSE_CURSORS = 105;
    
    public static final int FUNC_CANCEL_ALL = 120;
    
    public static final int FUNC_CLIENT_FEATURES = 191;
    
    public static final int FUNC_SET_END_TO_END_ATTR = 135;

    public static final int EXEC_OPTION_PARSE = 0x01;
    public static final int EXEC_OPTION_BIND = 0x08;
    public static final int EXEC_OPTION_EXECUTE = 0x20;
    public static final int EXEC_OPTION_FETCH = 0x40;
    public static final int EXEC_OPTION_COMMIT = 0x100;
    public static final int EXEC_OPTION_NOT_PLSQL = 0x8000;

    public static final int EXEC_FLAGS_IMPLICIT_RESULTSET = 0x00000001;

    public static final int TNS_MAX_SHORT_LENGTH = 252;
    public static final int TNS_LONG_LENGTH_INDICATOR = 254;
    public static final int TNS_NULL_LENGTH_INDICATOR = 255;
    public static final int TNS_CHUNK_SIZE = 32767;

    public static final int ERR_NO_DATA_FOUND = 1403;

    public static final int ORA_TYPE_NUM_VARCHAR = 1;
    public static final int ORA_TYPE_NUM_NUMBER = 2;
    public static final int ORA_TYPE_NUM_DATE = 12;
    
    public static final int ORA_TYPE_NUM_RAW = 23;
    
    public static final int ORA_TYPE_NUM_TIMESTAMP = 180;
    // A REF CURSOR OUT parameter's own bind descriptor -- the client still sends a placeholder
    // "value" for it (same as any OUT param, real or not) that needs decoding-not-refusing so the
    // byte stream stays in sync with whatever follows, same discipline as mssqlwire's BY_REF fix.
    public static final int ORA_TYPE_NUM_CURSOR = 102;

    private TtcConstants() {
    }
}
